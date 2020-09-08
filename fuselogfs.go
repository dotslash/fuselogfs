package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
	"github.com/pkg/xattr"
	"io"
	"io/ioutil"
	"os"
	"os/user"
	"path"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const RootInodeId = 1
const cacheDuration = 100 * time.Millisecond

type Inode struct {
	id    fuseops.InodeID
	paths []string
	count uint64
}

func (in *Inode) removePath(pathToRem string) (ok bool) {
	toRem := -1
	for i, p := range in.paths {
		if p == pathToRem {
			toRem = i
			break
		}
	}
	if toRem == -1 {
		return false
	}
	l := len(in.paths)
	in.paths[toRem] = in.paths[l-1]
	in.paths = in.paths[:l-1]
	return true
}

type FileWriteHandle struct {
	inode fuseops.InodeID
	id    fuseops.HandleID
	file  *os.File
	mu    sync.RWMutex
}

func (fh *FileWriteHandle) ensureFile(fs *FuseLogFs) (*os.File, error) {
	if fh.file != nil {
		return fh.file, nil
	}

	fh.mu.Lock()
	defer fh.mu.Unlock()

	if fh.file != nil {
		return fh.file, nil
	}

	_, stageFile, err := fs.getInodeFromId(fh.inode)
	if err != nil {
		return nil, err
	}

	fh.file, err = os.OpenFile(*stageFile, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	return fh.file, nil

}

type FuseLogFs struct {
	fuseutil.FileSystem

	stageDir string
	// TODO: I need to access inodes, pathToInode using a lock.
	inodes      map[fuseops.InodeID]*Inode
	pathToInode map[string]fuseops.InodeID

	lastInode        fuseops.InodeID
	lastHandleId     fuseops.HandleID
	fileWriteHandles map[fuseops.HandleID]*FileWriteHandle

	// Mutex for file system operations (adding nodes, incrementing inode number etc)
	fsmu sync.Mutex
	// Mutex for file operations (unlink, write etc)
	filemu sync.Mutex

	// UID and GID of the user that is running the the fuse fs.
	uid uint32
	gid uint32
}

func NewFuseLogFs(stageDir string, usr *user.User) (*FuseLogFs, error) {
	uid, err := strconv.ParseUint(usr.Uid, 10, 32)
	if err != nil {
		return nil, err
	}
	gid, err := strconv.ParseUint(usr.Gid, 10, 32)
	if err != nil {
		return nil, err
	}

	ret := &FuseLogFs{
		stageDir:         stageDir,
		pathToInode:      map[string]fuseops.InodeID{},
		inodes:           map[fuseops.InodeID]*Inode{},
		fileWriteHandles: map[fuseops.HandleID]*FileWriteHandle{},
		uid:              uint32(uid),
		gid:              uint32(gid),
	}
	ret.init()
	return ret, nil
}

func (fs *FuseLogFs) init() {
	fs.addNewInodeInternal(RootInodeId, "/")
	root, _ := fs.inodes[RootInodeId]
	root.count = 1 // Root has a reference count of 1 by default.

	fs.lastInode = 1
	fs.lastHandleId = 0
	logger.Error().Msg("INIT Done")
}

// ====== HELPERS =======

func (fs *FuseLogFs) osStatToInodeAttrs(stat os.FileInfo, node *Inode) fuseops.InodeAttributes {
	return fuseops.InodeAttributes{
		Size:   uint64(stat.Size()),
		Nlink:  uint32(len(node.paths)),
		Mode:   stat.Mode(),
		Atime:  stat.ModTime(),
		Mtime:  stat.ModTime(),
		Ctime:  stat.ModTime(),
		Crtime: time.Unix(0, 0), // TODO: creation time.
		Uid:    502,             // TODO: This is hardcoded to my current values
		Gid:    20,              // TODO: ^
	}
}

// TODO: Im reasonably certain Im not doing ref counting properly. It does not seem to hurt though.
func (fs *FuseLogFs) updateRefCount(inode *Inode, change uint64) {
	fs.fsmu.Lock()
	defer fs.fsmu.Unlock()
	inode.count += change
	newCnt := inode.count
	if newCnt == 0 {
		logger.Info().Msgf("Removing %+v", inode)
		fs.rmInode(inode)
	}
}

// TODO: Better name for "stage". What I call stage is the reality and the fuse fs is the putting up a
//  performance on a "stage" (i.e mount path) !
func (fs *FuseLogFs) stagePath(inode *Inode) string {
	return filePath(fs.stageDir, inode.paths[0])
}

func (fs *FuseLogFs) parentInodeId(inode *Inode) *fuseops.InodeID {
	if inode.paths[0] == "/" || inode.paths[0] == "" {
		return nil
	}
	par, _ := path.Split(inode.paths[0])
	parid, _ := fs.pathToInode[par]
	return &parid
}

// Does exactly what it says. Use only if addNewOrReuseStaleInode does not work for your use case.
func (fs *FuseLogFs) addNewInodeInternal(id fuseops.InodeID, path string) *Inode {
	newNode := &Inode{id: id, paths: []string{path}}
	fs.inodes[id] = newNode
	fs.pathToInode[path] = id
	logger.Info().Msgf("Adding inode %+v", newNode)
	return newNode
}

// If there is no inode for this path, it creates a new one. If there is a stale inode for this path
// it reuses the stale inode
// TODO: I think the resuse path is never taken, because reusing stale inodes will cause incorrect behaviour
//  for hard linking.
func (fs *FuseLogFs) addNewOrReuseStaleInode(path string) *Inode {
	inode, oldIsValid := fs.getInodeFromPath(path)
	if oldIsValid {
		return nil
	}
	if inode != nil {
		inode.paths = append(inode.paths, path)
		fs.pathToInode[path] = inode.id
		logger.Info().Msgf("Reusing stale inode %+v", inode)
		return inode
	}
	return fs.addNewInodeInternal(fs.newInodeNum(), path)
}

func (fs *FuseLogFs) getOrCreateInode(path string) *Inode {
	inode, oldIsValid := fs.getInodeFromPath(path)
	if oldIsValid {
		return inode
	}
	return fs.addNewOrReuseStaleInode(path)
}

func (fs *FuseLogFs) rmInode(inode *Inode) {
	logger.Info().Msgf("rmInode : %+v", inode)
	for _, inodePath := range inode.paths {
		delete(fs.pathToInode, inodePath)
	}
	delete(fs.inodes, inode.id)
}

// valid: true if the node is associated with this inode.
// This can be false for a Unlink/Rename/RmDir is done on this inode before we remove the
// inode in the subsequent ForgetInode call.
func (fs *FuseLogFs) getInodeFromPath(path string) (_inode *Inode, _valid bool) {
	nodeId, ok := fs.pathToInode[path]
	if !ok {
		// InodeId not found
		return nil, false
	}
	node, ok := fs.inodes[nodeId]
	if !ok {
		// Node does not exist for the inode id.
		// TODO: This should really not happen. Add a warning log?
		return nil, false
	}
	// Set valid
	valid := false
	for _, p := range node.paths {
		if p == path {
			valid = true
			break
		}
	}
	return node, valid
}

// TODO: Stop using the second param in favor fs.stagePath
// TODO: Be consistent about naming convention for return values
//  a. Use return values freely in the underlying function (or)
//  b. Use them purely for documentation purposes (like in this case _stagePath for documentation and stagePath
//     in the function body.
// Unlike getInodeFromPath, this does not need valid because an inode can be valid/invalid w.r.t a path
// Here we are fetcing the inode from the id.
func (fs *FuseLogFs) getInodeFromId(id fuseops.InodeID) (_inode *Inode, _stagePath *string, _err error) {
	node, ok := fs.inodes[id]
	if !ok {
		return nil, nil, syscall.ENOENT
	} else if len(node.paths) == 0 {
		logger.Info().Msgf("Treating stale node as ENOENT %+v", node)
		return nil, nil, syscall.ENOENT
	}
	stagePath := filePath(fs.stageDir, node.paths[0])
	return node, &stagePath, nil
}

func (fs *FuseLogFs) newInodeNum() fuseops.InodeID {
	fs.fsmu.Lock()
	defer fs.fsmu.Unlock()
	fs.lastInode++
	return fs.lastInode
}

func (fs *FuseLogFs) newHandleNum() fuseops.HandleID {
	fs.fsmu.Lock()
	defer fs.fsmu.Unlock()
	fs.lastHandleId++
	return fs.lastHandleId
}

// ====== LINKS =======

func (fs *FuseLogFs) CreateLink(_ context.Context, op *fuseops.CreateLinkOp) (err error) {
	defer func() {
		logger.Err(err).Msgf("CreateLink end:  -> %+v", op)
	}()

	// Get par node info.
	parNode, _, err := fs.getInodeFromId(op.Parent)
	if err != nil {
		return err
	}
	// Target node ("old node")
	targetNode, _, err := fs.getInodeFromId(op.Target)
	if err != nil {
		return err
	}

	// Perform operation on stage directory
	oldStatePath := fs.stagePath(targetNode)
	newStagePath := filePath(fs.stagePath(parNode), op.Name)
	err = os.Link(oldStatePath, newStagePath)
	if err != nil {
		return err
	}
	newStat, err := os.Stat(newStagePath)
	if err != nil {
		return err
	}

	// Update inode maps.
	newPath := filePath(parNode.paths[0], op.Name)
	fs.pathToInode[newPath] = targetNode.id
	targetNode.paths = append(targetNode.paths, newPath)

	op.Entry = fuseops.ChildInodeEntry{
		Child:                targetNode.id,
		Attributes:           fs.osStatToInodeAttrs(newStat, targetNode),
		AttributesExpiration: time.Time{},
		EntryExpiration:      time.Time{},
	}

	return nil
}

func (fs *FuseLogFs) CreateSymlink(_ context.Context, op *fuseops.CreateSymlinkOp) (err error) {
	defer func() {
		logger.Err(err).Msgf("CreateSymLink end:  -> %+v", op)
	}()

	// Get par node info.
	parNode, _, err := fs.getInodeFromId(op.Parent)
	if err != nil {
		return err
	}
	newFullPath := filePath(fs.stagePath(parNode), op.Name)
	return os.Symlink(op.Target, newFullPath)
}

func (fs *FuseLogFs) ReadSymlink(_ context.Context, op *fuseops.ReadSymlinkOp) error {
	node, _, err := fs.getInodeFromId(op.Inode)
	if err != nil {
		return err
	}
	op.Target, err = os.Readlink(fs.stagePath(node))
	return err
}

// ====== GENERAL =======

func (fs *FuseLogFs) ForgetInode(_ context.Context, op *fuseops.ForgetInodeOp) error {
	// TODO: Accessing fs.inodes directly because getInodeFromId will not return the inode
	//  if inode.paths is empty. Fix this.
	inode, ok := fs.inodes[op.Inode]
	if !ok {
		return syscall.ENOENT
	}
	fs.updateRefCount(inode, -op.N)
	return nil
}

func (fs *FuseLogFs) LookUpInode(_ context.Context, op *fuseops.LookUpInodeOp) error {
	// Get par node info.
	parNode, parStagePath, err := fs.getInodeFromId(op.Parent)
	if err != nil {
		return err
	}
	// Check the stage paths[0] of the child node exists.
	stagePath := filePath(*parStagePath, op.Name)
	stat, err := os.Stat(stagePath)
	if err != nil {
		return mapOsStatError(err)
	}
	// Stage has this file. So we must behave as if the file exists.
	// Get/Create Inode for the child entry and set the Attributes in response.
	node := fs.getOrCreateInode(filePath(parNode.paths[0], op.Name))
	fs.updateRefCount(node, 1)
	op.Entry = fuseops.ChildInodeEntry{
		Child:                node.id,
		Attributes:           fs.osStatToInodeAttrs(stat, node),
		AttributesExpiration: time.Now().Add(cacheDuration),
		EntryExpiration:      time.Now().Add(cacheDuration),
	}
	return nil
}

func mapOsStatError(err error) error {
	if os.IsNotExist(err) {
		return syscall.ENOENT
	}
	return err
}

func (fs *FuseLogFs) Rename(_ context.Context, op *fuseops.RenameOp) (err error) {
	defer func() {
		logger.Err(err).Msgf("Rename end:  -> %+v", op)
	}()

	fs.filemu.Lock()
	defer fs.filemu.Unlock()

	oldParNode, _, err := fs.getInodeFromId(op.OldParent)
	if err != nil {
		return err
	}
	newParNode, _, err := fs.getInodeFromId(op.NewParent)
	if err != nil {
		return err
	}
	err = os.Rename(
		filePath(fs.stagePath(oldParNode), op.OldName),
		filePath(fs.stagePath(newParNode), op.NewName))
	if err != nil {
		return err
	}
	// Dont create the inode for new file because we will generate one when we get a
	// lookup call.
	return fs.UnlinkInternal(op.OldParent, op.OldName)
}

// ====== FILE =======

func (fs *FuseLogFs) getWriteFileHandle(
	id fuseops.InodeID, handleID fuseops.HandleID) (*FileWriteHandle, error) {
	_, _, err := fs.getInodeFromId(id)
	if err != nil {
		return nil, err
	}
	handle, ok := fs.fileWriteHandles[handleID]
	if !ok || id != handle.inode {
		return nil, syscall.EINVAL
	}

	_, err = handle.ensureFile(fs)
	if err != nil {
		return nil, err
	}
	return handle, nil
}

func (fs *FuseLogFs) CreateFile(_ context.Context, op *fuseops.CreateFileOp) (err error) {
	defer func() {
		logger.Err(err).Msgf("CreateFile end:  -> %+v", op)
	}()

	fs.filemu.Lock()
	defer fs.filemu.Unlock()

	parNode, _, err := fs.getInodeFromId(op.Parent)
	if err != nil {
		return err
	}
	parStagePath := fs.stagePath(parNode)
	childStagePath := filePath(parStagePath, op.Name)
	childPath := filePath(parNode.paths[0], op.Name)

	_, err = os.Stat(childStagePath)
	if err == nil {
		return syscall.EEXIST
	} else if !os.IsNotExist(err) {
		return err
	} else if _, valid := fs.getInodeFromPath(childPath); valid {
		return syscall.EEXIST
	}

	handle := fs.newHandleNum()
	newNode := fs.addNewOrReuseStaleInode(childPath)
	fs.updateRefCount(newNode, 1)
	if newNode == nil {
		return syscall.EIO
	}

	f, err := os.OpenFile(childStagePath, os.O_RDWR|os.O_CREATE, 0666)
	if err == nil {
		err = f.Chmod(op.Mode)
	}
	if err != nil {
		return err
	}

	fs.fileWriteHandles[handle] = &FileWriteHandle{
		inode: newNode.id,
		id:    handle,
		file:  f,
	}

	stat, err := os.Stat(childStagePath)
	if err != nil {
		return err
	}
	op.Handle = handle
	op.Entry = fuseops.ChildInodeEntry{
		Child:                newNode.id,
		Attributes:           fs.osStatToInodeAttrs(stat, newNode),
		AttributesExpiration: time.Now().Add(cacheDuration),
		EntryExpiration:      time.Now().Add(cacheDuration),
	}
	return nil
}

func (*FuseLogFs) MkNode(context.Context, *fuseops.MkNodeOp) error {
	return errors.New("implement me. why? ")
}

func (fs *FuseLogFs) Fallocate(_ context.Context, op *fuseops.FallocateOp) error {
	return errors.New("implement me. why? ")
}

func (fs *FuseLogFs) FlushFile(_ context.Context, op *fuseops.FlushFileOp) (err error) {
	defer func() {
		errOrDebug(&logger, err).Msgf("FlushFile end:  -> %+v", op)
	}()

	fs.filemu.Lock()
	defer fs.filemu.Unlock()

	handle, ok := fs.fileWriteHandles[op.Handle]
	if !ok {
		// Nothing to do.
		return syscall.EIO
	} else if handle.file == nil {
		return nil
	}
	return handle.file.Close()
}

func (fs *FuseLogFs) OpenFile(_ context.Context, op *fuseops.OpenFileOp) (err error) {
	defer func() {
		errOrDebug(&logger, err).Msgf("OpenFile end:  -> %+v", op)
	}()

	node, _, err := fs.getInodeFromId(op.Inode)
	if err != nil {
		return err
	}
	if _, err = os.Stat(fs.stagePath(node)); err != nil {
		return mapOsStatError(err)
	}
	op.Handle = fs.newHandleNum()
	fs.fileWriteHandles[op.Handle] = &FileWriteHandle{
		inode: op.Inode,
		id:    op.Handle,
		file:  nil,
	}
	return nil
}

func (fs *FuseLogFs) WriteFile(_ context.Context, op *fuseops.WriteFileOp) (err error) {
	defer func() {
		logger.Err(err).Msgf("WriteFile end:  -> %v", writeFileStr(op))
	}()

	fs.filemu.Lock()
	defer fs.filemu.Unlock()

	handle, err := fs.getWriteFileHandle(op.Inode, op.Handle)
	if err != nil {
		return err
	}
	_, err = handle.file.WriteAt(op.Data, op.Offset)
	return err
}

func (fs *FuseLogFs) SyncFile(_ context.Context, op *fuseops.SyncFileOp) (err error) {
	defer func() {
		logger.Err(err).Msgf("WriteFile end:  -> %+v", op)
	}()

	fs.filemu.Lock()
	defer fs.filemu.Unlock()

	handle, err := fs.getWriteFileHandle(op.Inode, op.Handle)
	if err != nil {
		return err
	}
	return handle.file.Sync()
}

func (fs *FuseLogFs) ReleaseFileHandle(_ context.Context, op *fuseops.ReleaseFileHandleOp) error {
	delete(fs.fileWriteHandles, op.Handle)
	return nil
}

func (fs *FuseLogFs) ReadFile(_ context.Context, op *fuseops.ReadFileOp) (err error) {
	defer func() {
		errOrDebug(&logger, err).Msgf("ReadFile end:  -> %v", readFileStr(op))
	}()
	node, _, err := fs.getInodeFromId(op.Inode)
	if err != nil {
		return err
	}
	f, err := os.Open(fs.stagePath(node))
	if err != nil {
		return err
	}
	op.BytesRead, err = f.ReadAt(op.Dst, op.Offset)
	if err == io.EOF {
		err = nil // EOF is fine. sys call has no equivalent.
	}
	return err
}

func (fs *FuseLogFs) Unlink(_ context.Context, op *fuseops.UnlinkOp) error {
	fs.filemu.Lock()
	defer fs.filemu.Unlock()
	return fs.UnlinkInternal(op.Parent, op.Name)
}

func (fs *FuseLogFs) UnlinkInternal(parentId fuseops.InodeID, nameToUnlink string) (err error) {
	defer func() {
		logger.Info().Msgf("UnlinkInternal end:  -> %v %v %v", parentId, nameToUnlink, err)
	}()

	parNode, _, err := fs.getInodeFromId(parentId)
	if err != nil {
		return err
	}

	childNode, childValid := fs.getInodeFromPath(filePath(parNode.paths[0], nameToUnlink))
	childPath := filePath(parNode.paths[0], nameToUnlink)
	childStagePath := filePath(fs.stagePath(parNode), nameToUnlink)
	if !childValid {
		return syscall.ENOENT
	}
	err = os.Remove(childStagePath)
	if err != nil && !os.IsNotExist(err) {
		// If item does not exist, dont quit here.
		// Remove the entry from fuse also.
		return err
	}
	if !childNode.removePath(childPath) {
		return syscall.ENOENT
	}
	delete(fs.pathToInode, childPath)
	// After this we will still have this inode in the inodes maps
	// - For non hard linked items, Inode.paths []. In this case we are assuming
	//   a subsequent call to forget inode will result in the inode being gone.
	// - For hard linked files, path will be non empty.
	return nil
}

// ====== DIR =======

func (fs *FuseLogFs) ListFiles(dir fuseops.InodeID) ([]*fuseutil.Dirent, error) {
	node, _, err := fs.getInodeFromId(dir)
	if err != nil {
		return nil, err
	}
	files, err := ioutil.ReadDir(fs.stagePath(node))
	if err != nil {
		return nil, err
	}
	ret := make([]*fuseutil.Dirent, 0, len(files)+2)
	ret = append(ret, &fuseutil.Dirent{
		Offset: 1,
		Inode:  node.id,
		Name:   ".",
		Type:   fuseutil.DT_Directory,
	})
	ret = append(ret, &fuseutil.Dirent{
		Offset: 2,
		Name:   "..",
		Type:   fuseutil.DT_Directory,
	})
	parIdPtr := fs.parentInodeId(node)
	if parIdPtr != nil {
		ret[1].Inode = *parIdPtr
	}

	for _, f := range files {
		// these paths are relative.
		itemType := fuseutil.DT_File // TODO: links. Done?

		if f.Mode()&os.ModeSymlink == os.ModeSymlink {
			itemType = fuseutil.DT_Link
		} else if f.IsDir() {
			itemType = fuseutil.DT_Directory
		}

		ent := &fuseutil.Dirent{
			Inode: fs.getOrCreateInode(filePath(node.paths[0], f.Name())).id,
			Name:  f.Name(),
			Type:  itemType,
		}
		ret = append(ret, ent)
		ent.Offset = fuseops.DirOffset(len(ret) + 1)
	}
	return ret, nil
}

func (fs *FuseLogFs) MkDir(_ context.Context, op *fuseops.MkDirOp) error {
	parNode, parStage, err := fs.getInodeFromId(op.Parent)
	if err != nil {
		return err
	}

	childStagePath := filePath(*parStage, op.Name)
	childPath := filePath(parNode.paths[0], op.Name)
	_, alreadyExists := fs.getInodeFromPath(childPath)
	if alreadyExists {
		return syscall.EEXIST
	}
	err = os.Mkdir(childStagePath, op.Mode)
	if err != nil {
		return err
	}
	stat, err := os.Stat(childStagePath)
	if err != nil {
		return err
	}

	childNode := fs.addNewOrReuseStaleInode(childPath)
	if childNode == nil {
		return syscall.EIO
	}
	op.Entry = fuseops.ChildInodeEntry{
		Child:                childNode.id,
		Attributes:           fs.osStatToInodeAttrs(stat, childNode),
		AttributesExpiration: time.Now().Add(cacheDuration),
		EntryExpiration:      time.Now().Add(cacheDuration),
	}
	return nil
}

func (fs *FuseLogFs) OpenDir(_ context.Context, op *fuseops.OpenDirOp) error {
	_, stagePath, err := fs.getInodeFromId(op.Inode)
	if err != nil {
		return err
	}

	stat, err := os.Stat(*stagePath)
	if err != nil {
		return mapOsStatError(err)
	}
	if !stat.IsDir() {
		return syscall.ENOTDIR
	}
	op.Handle = fs.newHandleNum()
	return nil
}

func (fs *FuseLogFs) RmDir(_ context.Context, op *fuseops.RmDirOp) error {
	fs.filemu.Lock()
	defer fs.filemu.Unlock()

	parNode, _, err := fs.getInodeFromId(op.Parent)
	if err != nil {
		return err
	}
	childPath := filePath(parNode.paths[0], op.Name)
	childNode, childIsValid := fs.getInodeFromPath(childPath)
	if !childIsValid {
		return syscall.ENOENT
	}
	err = os.Remove(fs.stagePath(childNode))
	if err != nil {
		return err
	}
	// We will not have any hardlinks here.
	// This is equivalent to the non-hardlink case in Unlink
	childNode.paths = []string{}
	delete(fs.pathToInode, childPath)
	return nil
}

func (fs *FuseLogFs) ReadDir(_ context.Context, op *fuseops.ReadDirOp) error {
	ls, err := fs.ListFiles(op.Inode)
	if err != nil {
		return err
	}
	op.BytesRead = 0
	for i, item := range ls {
		// NOTE: This can be efficient if we start iterating from the correct
		// offset. But :/
		if fuseops.DirOffset(i) <= op.Offset {
			continue
		}
		cur := fuseutil.WriteDirent(op.Dst[op.BytesRead:], *item)
		if cur == 0 {
			// No more entries in this readdir page.
			break
		}
		op.BytesRead += cur

	}
	return nil
}

func (*FuseLogFs) ReleaseDirHandle(context.Context, *fuseops.ReleaseDirHandleOp) error {
	return nil
}

// ====== ATTRIBUTES =======

func (fs *FuseLogFs) GetInodeAttributes(_ context.Context, op *fuseops.GetInodeAttributesOp) error {
	node, _, err := fs.getInodeFromId(op.Inode)
	if err != nil {
		return err
	}

	stat, err := os.Stat(fs.stagePath(node))
	if err != nil {
		return mapOsStatError(err)
	}

	op.Attributes = fs.osStatToInodeAttrs(stat, node)
	op.AttributesExpiration = time.Now().Add(cacheDuration)
	return nil
}

func (fs *FuseLogFs) SetInodeAttributes(_ context.Context, op *fuseops.SetInodeAttributesOp) error {
	node, _, err := fs.getInodeFromId(op.Inode)
	if err != nil {
		return err
	}
	stagePath := fs.stagePath(node)

	// Chmod if needed.
	if op.Mode != nil {
		err = os.Chmod(stagePath, *op.Mode)
		if err != nil {
			return err
		}
	}

	// Chtimes if needed.
	if op.Atime != nil || op.Mtime != nil {
		statBefore, err := os.Stat(stagePath)
		if err != nil {
			return mapOsStatError(err)
		}
		newAtime, newMtime := statBefore.ModTime(), statBefore.ModTime()
		if op.Atime != nil {
			newAtime = *op.Atime
		}
		if op.Mtime != nil {
			newMtime = *op.Mtime
		}
		if os.Chtimes(stagePath, newAtime, newMtime) != nil {
			return err
		}
	}

	// os.Stat and set the return values.
	stat, err := os.Stat(fs.stagePath(node))
	if err != nil {
		return mapOsStatError(err)
	}

	op.Attributes = fs.osStatToInodeAttrs(stat, node)
	op.AttributesExpiration = time.Now().Add(cacheDuration)
	return nil
}

// ====== XATTRS =======

func (fs *FuseLogFs) SetXattr(_ context.Context, op *fuseops.SetXattrOp) error {
	_, stagePath, err := fs.getInodeFromId(op.Inode)
	if err != nil {
		return err
	}
	_, err = xattr.Get(*stagePath, op.Name)
	if op.Flags == 0x1 && err == nil {
		return syscall.EEXIST
	} else if op.Flags == 0x2 && err != nil {
		return syscall.ENOATTR
	}
	return xattr.Set(*stagePath, op.Name, op.Value)
}

func (fs *FuseLogFs) RemoveXattr(_ context.Context, op *fuseops.RemoveXattrOp) error {
	_, stagePath, err := fs.getInodeFromId(op.Inode)
	if err != nil {
		return err
	}
	return xattr.Remove(*stagePath, op.Name)
}

func (fs *FuseLogFs) GetXattr(_ context.Context, op *fuseops.GetXattrOp) error {
	_, stagePath, err := fs.getInodeFromId(op.Inode)
	if err != nil {
		return err
	}
	val, err := xattr.Get(*stagePath, op.Name)
	if err != nil {
		return err
	}
	copy(op.Dst, val)
	op.BytesRead = len(val)
	return nil
}

func (fs *FuseLogFs) ListXattr(_ context.Context, op *fuseops.ListXattrOp) error {
	_, stagePath, err := fs.getInodeFromId(op.Inode)
	if err != nil {
		return err
	}
	xattrs, err := xattr.List(*stagePath)
	if err != nil {
		return err
	}
	dst := op.Dst[:]
	for _, key := range xattrs {
		keyLen := len(key) + 1
		if len(dst) >= keyLen {
			copy(dst, key)
			dst = dst[keyLen:]
		} else if len(op.Dst) != 0 {
			return syscall.ERANGE
		}
		op.BytesRead += keyLen
	}
	return nil
}

// ====== OTHERS =======

func (*FuseLogFs) StatFS(_ context.Context, op *fuseops.StatFSOp) error {
	kSize := uint64(1 << 40) // 1 TB
	kBlockSize := uint32(4096)
	numBlocks := kSize / uint64(kBlockSize)

	op.BlockSize = kBlockSize
	op.Blocks = numBlocks
	op.BlocksFree = numBlocks
	op.BlocksAvailable = numBlocks

	op.IoSize = 1 << 20 // 1MB
	op.Inodes = 1 << 20 // ~1 Million
	op.InodesFree = op.Inodes
	return nil
}

func (*FuseLogFs) Destroy() {
}

// Misc functions

// TODO: Why not filepath.Join?
func filePath(parPath, childName string) string {
	c := strings.HasPrefix(childName, "/")
	p := strings.HasSuffix(parPath, "/")
	if p && c {
		return parPath + childName[1:]
	} else if p || c {
		return parPath + childName
	} else {
		return parPath + "/" + childName
	}
}

// "Short" debug string for ReadFileOp.
func readFileStr(op *fuseops.ReadFileOp) string {
	return fmt.Sprintf(
		"Inode:%v Handle:%v, Offset:%v Dst len:%v BytesRead:%v",
		op.Inode, op.Handle, op.Offset, len(op.Dst), op.BytesRead)
}

// "Short" debug string for WriteFileOp.
func writeFileStr(op *fuseops.WriteFileOp) string {
	return fmt.Sprintf(
		"Inode:%v Handle:%v, Offset:%v Data len:%v",
		op.Inode, op.Handle, op.Offset, len(op.Data))
}
