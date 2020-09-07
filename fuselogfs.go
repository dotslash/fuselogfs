package main

import (
	"context"
	"errors"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
	"github.com/pkg/xattr"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"
)

const RootInodeId = 1
const cacheDuration = 10 * time.Second

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

	_, stageFile, err := fs.getStageFileAndInode(fh.inode)
	if err != nil {
		return nil, err
	}
	//stat, err := os.Stat(*stageFile)
	//if err != nil {
	//	return nil, err
	//}
	//if stat.Mode() & os.O_WRONLY == os.O_WRONLY {
	//
	//}

	fh.file, err = os.OpenFile(*stageFile, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	return fh.file, nil

}

type FuseLogFs struct {
	fuseutil.FileSystem

	stageDir    string
	inodes      map[fuseops.InodeID]*Inode
	pathToInode map[string]fuseops.InodeID

	lastInode        fuseops.InodeID
	lastHandleId     fuseops.HandleID
	fileWriteHandles map[fuseops.HandleID]*FileWriteHandle
	fsmu             sync.Mutex
	filemu           sync.Mutex
	count            int
}

func (fs *FuseLogFs) Init() {
	fs.addNewInodeInternal(RootInodeId, "/")
	root, _ := fs.inodes[RootInodeId]
	root.count = 1 // Root has a reference count of 1 by default.

	fs.lastInode = 1
	fs.lastHandleId = 0
	logger.Error().Msg("INIT Done")
}

func (fs *FuseLogFs) updateRefCount(inode *Inode, change uint64) {
	fs.fsmu.Lock()
	defer fs.fsmu.Unlock()
	inode.count += change
	newCnt := inode.count
	if newCnt == 0 {
		fs.rmInode(inode)
	}
}

// ====== HELPERS =======

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

func (fs *FuseLogFs) addNewInodeInternal(id fuseops.InodeID, path string) *Inode {
	newNode := &Inode{id: id, paths: []string{path}}
	fs.inodes[id] = newNode
	fs.pathToInode[path] = id
	return newNode
}
func (fs *FuseLogFs) addInode(path string) *Inode {
	inode, oldIsValid := fs.getInode(path)
	if oldIsValid {
		return nil
	}
	if inode != nil {
		inode.paths = append(inode.paths, path)
		fs.pathToInode[path] = inode.id
		return inode
	}
	return fs.addNewInodeInternal(fs.newInodeNum(), path)
}

func (fs *FuseLogFs) getOrCreateInode(path string) *Inode {
	inode, oldIsValid := fs.getInode(path)
	if oldIsValid {
		return inode
	}
	return fs.addInode(path)
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

func (fs *FuseLogFs) rmInode(inode *Inode) {
	for _, inodePath := range inode.paths {
		delete(fs.pathToInode, inodePath)
	}
	delete(fs.inodes, inode.id)
}

func (fs *FuseLogFs) getInode(path string) (inode *Inode, valid bool) {
	nodeId, ok := fs.pathToInode[path]
	if !ok {
		return nil, false
	}
	node, _ := fs.inodes[nodeId]
	return node, len(node.paths) > 0
}

func (fs *FuseLogFs) getStageFileAndInode(id fuseops.InodeID) (*Inode, *string, error) {
	node, ok := fs.inodes[id]
	logger.Info().Msgf("fs.inodes[id]: %+v %v", node, ok)
	if !ok {
		return nil, nil, syscall.ENOENT
	}
	stagePath := fs.stageDir + "/" + node.paths[0]
	logger.Info().Msgf("stagePath: %v", stagePath)
	return node, &stagePath, nil
}

// ====== LINKS =======

func (fs *FuseLogFs) CreateLink(_ context.Context, op *fuseops.CreateLinkOp) error {
	// Get par node info.
	parNode, _, err := fs.getStageFileAndInode(op.Parent)
	if err != nil {
		return err
	}
	// Target node ("old node")
	targetNode, _, err := fs.getStageFileAndInode(op.Target)
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
		Attributes:           osStatToInodeAttrs(newStat, targetNode),
		AttributesExpiration: time.Time{},
		EntryExpiration:      time.Time{},
	}

	return nil
}

func (fs *FuseLogFs) CreateSymlink(_ context.Context, op *fuseops.CreateSymlinkOp) error {
	// Get par node info.
	parNode, _, err := fs.getStageFileAndInode(op.Parent)
	if err != nil {
		return err
	}
	newFullPath := filePath(fs.stagePath(parNode), op.Name)
	return os.Symlink(op.Target, newFullPath)
}

func (fs *FuseLogFs) ReadSymlink(_ context.Context, op *fuseops.ReadSymlinkOp) error {
	node, _, err := fs.getStageFileAndInode(op.Inode)
	if err != nil {
		return err
	}
	op.Target, err = os.Readlink(fs.stagePath(node))
	return err
}

// ====== GENERAL =======

func (fs *FuseLogFs) ForgetInode(_ context.Context, op *fuseops.ForgetInodeOp) error {
	inode, ok := fs.inodes[op.Inode]
	if !ok {
		return syscall.ENOENT
	}
	fs.updateRefCount(inode, -op.N)
	return nil
}

func (fs *FuseLogFs) LookUpInode(_ context.Context, op *fuseops.LookUpInodeOp) error {
	// Get par node info.
	parNode, parStagePath, err := fs.getStageFileAndInode(op.Parent)
	if err != nil {
		return err
	}
	// Check the stage paths[0] of the child node exists.
	stagePath := filePath(*parStagePath, op.Name)
	stat, err := os.Stat(stagePath)
	if err != nil {
		logger.Info().Msgf("%v", err)
		return mapOsStatError(err)
	}
	// Get/Create Inode for the child entry and set the Attributes in response.
	node := fs.getOrCreateInode(filePath(parNode.paths[0], op.Name))
	fs.updateRefCount(node, 1)
	op.Entry = fuseops.ChildInodeEntry{
		Child:                node.id,
		Attributes:           osStatToInodeAttrs(stat, node),
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

func (fs *FuseLogFs) Rename(_ context.Context, op *fuseops.RenameOp) error {
	fs.filemu.Lock()
	defer fs.filemu.Unlock()

	oldParNode, _, err := fs.getStageFileAndInode(op.OldParent)
	if err != nil {
		return err
	}
	newParNode, _, err := fs.getStageFileAndInode(op.NewParent)
	if err != nil {
		return err
	}
	return os.Rename(
		filePath(fs.stagePath(oldParNode), op.OldName),
		filePath(fs.stagePath(newParNode), op.NewName))
}

// ====== FILE =======

func (fs *FuseLogFs) getWriteFileHandle(
	id fuseops.InodeID, handleID fuseops.HandleID) (*FileWriteHandle, error) {
	_, _, err := fs.getStageFileAndInode(id)
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

func (fs *FuseLogFs) CreateFile(_ context.Context, op *fuseops.CreateFileOp) error {
	fs.count += 1
	if fs.count > 10 {
		panic("too much")
	}
	fs.filemu.Lock()
	defer fs.filemu.Unlock()

	parNode, _, err := fs.getStageFileAndInode(op.Parent)
	if err != nil {
		return err
	}
	parStagePath := fs.stagePath(parNode)
	childStagePath := filePath(parStagePath, op.Name)
	childPath := filePath(parNode.paths[0], op.Name)

	_, err = os.Stat(childStagePath)
	if err == nil {
		logger.Info().Msgf("Remote EEXIST")
		return syscall.EEXIST
	} else if !os.IsNotExist(err) {
		return err
	} else if in, valid := fs.getInode(childPath); valid {
		logger.Info().Msgf("potentially weird eexist %v %v", in, valid)
		return syscall.EEXIST
	}

	handle := fs.newHandleNum()
	newNode := fs.addInode(childPath)
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
		Attributes:           osStatToInodeAttrs(stat, newNode),
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

func (fs *FuseLogFs) FlushFile(_ context.Context, op *fuseops.FlushFileOp) error {
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

func (fs *FuseLogFs) OpenFile(_ context.Context, op *fuseops.OpenFileOp) error {
	node, _, err := fs.getStageFileAndInode(op.Inode)
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

func (fs *FuseLogFs) WriteFile(_ context.Context, op *fuseops.WriteFileOp) error {
	fs.filemu.Lock()
	defer fs.filemu.Unlock()

	logger.Info().Msgf("WriteFile: %v %v %v", op.Handle, op.Inode, op.Offset)

	handle, err := fs.getWriteFileHandle(op.Inode, op.Handle)
	if err != nil {
		return err
	}
	_, err = handle.file.WriteAt(op.Data, op.Offset)
	return err
}

func (fs *FuseLogFs) SyncFile(_ context.Context, op *fuseops.SyncFileOp) error {
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

func (fs *FuseLogFs) ReadFile(_ context.Context, op *fuseops.ReadFileOp) error {
	node, _, err := fs.getStageFileAndInode(op.Inode)
	if err != nil {
		return err
	}
	f, err := os.Open(fs.stagePath(node))
	if err != nil {
		return err
	}
	op.BytesRead, err = f.ReadAt(op.Dst, op.Offset)
	return err
}

func (fs *FuseLogFs) Unlink(_ context.Context, op *fuseops.UnlinkOp) error {
	fs.filemu.Lock()
	defer fs.filemu.Unlock()
	parNode, _, err := fs.getStageFileAndInode(op.Parent)
	if err != nil {
		return err
	}

	childNode, childValid := fs.getInode(filePath(parNode.paths[0], op.Name))
	childPath := filePath(parNode.paths[0], op.Name)
	childStagePath := filePath(fs.stagePath(parNode), op.Name)
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
	// After this we will still have this file in the inode maps
	// All we are doing is remove the entry from Inode.paths
	// - For non hard linked items, Inode.paths []. In this case we are assuming
	//   a subsequent call to forget inode will result in the inode being gone.
	// - For hard linked files, path will be non empty.
	return nil
}

// ====== DIR =======

func (fs *FuseLogFs) ListFiles(dir fuseops.InodeID) ([]*fuseutil.Dirent, error) {
	node, _, err := fs.getStageFileAndInode(dir)
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
	parNode, parStage, err := fs.getStageFileAndInode(op.Parent)
	if err != nil {
		return err
	}

	childStagePath := filePath(*parStage, op.Name)
	childPath := filePath(parNode.paths[0], op.Name)
	_, alreadyExists := fs.getInode(childPath)
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

	childNode := fs.addInode(childPath)
	if childNode == nil {
		return syscall.EIO
	}
	op.Entry = fuseops.ChildInodeEntry{
		Child:                childNode.id,
		Attributes:           osStatToInodeAttrs(stat, childNode),
		AttributesExpiration: time.Now().Add(cacheDuration),
		EntryExpiration:      time.Now().Add(cacheDuration),
	}
	return nil
}

func (fs *FuseLogFs) OpenDir(_ context.Context, op *fuseops.OpenDirOp) error {
	_, stagePath, err := fs.getStageFileAndInode(op.Inode)
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

	parNode, _, err := fs.getStageFileAndInode(op.Parent)
	if err != nil {
		return err
	}
	childPath := filePath(parNode.paths[0], op.Name)
	childNode, childIsValid := fs.getInode(childPath)
	if !childIsValid {
		return syscall.ENOENT
	}
	err = os.Remove(fs.stagePath(childNode))
	if err != nil {
		return err
	}
	childNode.paths = []string{}
	return nil
}

func (fs *FuseLogFs) ReadDir(_ context.Context, op *fuseops.ReadDirOp) error {
	ls, err := fs.ListFiles(op.Inode)
	if err != nil {
		return err
	}
	op.BytesRead = 0
	for i, item := range ls {
		if fuseops.DirOffset(i) <= op.Offset {
			continue
		}
		logger.Info().Msgf("%+v", item)
		cur := fuseutil.WriteDirent(op.Dst[op.BytesRead:], *item)
		if cur == 0 {
			break
		}
		op.BytesRead += cur

	}
	logger.Info().Msgf("%+v", *op)
	return nil
}

func (*FuseLogFs) ReleaseDirHandle(context.Context, *fuseops.ReleaseDirHandleOp) error {
	return nil
}

// ====== ATTRIBUTES =======

func (fs *FuseLogFs) GetInodeAttributes(_ context.Context, op *fuseops.GetInodeAttributesOp) error {
	node, _, err := fs.getStageFileAndInode(op.Inode)
	if err != nil {
		return err
	}

	stat, err := os.Stat(fs.stagePath(node))
	if err != nil {
		return mapOsStatError(err)
	}

	op.Attributes = osStatToInodeAttrs(stat, node)
	op.AttributesExpiration = time.Now().Add(cacheDuration)
	return nil
}

func (fs *FuseLogFs) SetInodeAttributes(_ context.Context, op *fuseops.SetInodeAttributesOp) error {
	node, _, err := fs.getStageFileAndInode(op.Inode)
	if err != nil {
		return err
	}
	stagePath := fs.stagePath(node)

	if op.Mode != nil {
		err = os.Chmod(stagePath, *op.Mode)
		if err != nil {
			return err
		}
	}
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

	stat, err := os.Stat(fs.stagePath(node))
	if err != nil {
		return mapOsStatError(err)
	}

	op.Attributes = osStatToInodeAttrs(stat, node)
	op.AttributesExpiration = time.Now().Add(cacheDuration)
	return nil
}

// ====== XATTRS =======

func (fs *FuseLogFs) SetXattr(_ context.Context, op *fuseops.SetXattrOp) error {
	_, stagePath, err := fs.getStageFileAndInode(op.Inode)
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

func (*FuseLogFs) RemoveXattr(context.Context, *fuseops.RemoveXattrOp) error {
	return syscall.ENOATTR
}

func (fs *FuseLogFs) GetXattr(_ context.Context, op *fuseops.GetXattrOp) error {
	_, stagePath, err := fs.getStageFileAndInode(op.Inode)
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
	_, stagePath, err := fs.getStageFileAndInode(op.Inode)
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

func (*FuseLogFs) StatFS(ctx context.Context, op *fuseops.StatFSOp) error {
	kSize := uint64(1 << 40) // 1 TB
	kBlockSize := uint32(4096)
	numBlocks := kSize / uint64(kBlockSize)

	op.BlockSize = kBlockSize
	op.Blocks = numBlocks
	op.BlocksFree = numBlocks
	op.BlocksAvailable = numBlocks

	op.IoSize = 1 << 20 // 1MB
	op.Inodes = 1 << 20 // 1 Million
	op.InodesFree = op.Inodes
	return nil
}

func (*FuseLogFs) Destroy() {
}

// Misc functions

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

func osStatToInodeAttrs(stat os.FileInfo, node *Inode) fuseops.InodeAttributes {
	return fuseops.InodeAttributes{
		Size:   uint64(stat.Size()),
		Nlink:  uint32(len(node.paths)),
		Mode:   stat.Mode(),
		Atime:  stat.ModTime(),
		Mtime:  stat.ModTime(),
		Ctime:  stat.ModTime(),
		Crtime: time.Unix(0, 0), // TODO: creation time.
		Uid:    502,             // TODO: This is hardcoded to my current values
		Gid:    20,              // TODO
	}
}
