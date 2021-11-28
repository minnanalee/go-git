package main

import (
	"fmt"
	"github.com/emirpasic/gods/trees/binaryheap"
	"github.com/go-git/go-git/v5"
	. "github.com/go-git/go-git/v5/_examples"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/cache"
	commitgraph_fmt "github.com/go-git/go-git/v5/plumbing/format/commitgraph"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/object/commitgraph"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"io"
	"path"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/osfs"
)

type BigFile struct {
	Branch string
	Commit object.Commit
	File   object.File
}

// Example how to resolve a revision into its commit counterpart
func main() {

	timeStart := time.Now()
	//path := "D:\\ShWDaily\\1127\\flux-get-started\\flux-get-started" //os.Args[1]
	path := "D:\\ShWDaily\\1127\\3\\flux-get-started.git"
	shanghai, err := time.LoadLocation("Asia/Shanghai")
	fmt.Println("loacation", shanghai, err)
	since := time.Date(2021, 11, 27, 8, 0, 0, 0, shanghai)
	until := time.Date(2021, 11, 29, 8, 0, 0, 0, shanghai)

	// We instantiate a new repository targeting the given path (the .git folder)
	fs := osfs.New(path)
	if _, err := fs.Stat(git.GitDirName); err == nil {
		fs, err = fs.Chroot(git.GitDirName)
		CheckIfError(err)
	}

	s := filesystem.NewStorageWithOptions(fs, cache.NewObjectLRUDefault(), filesystem.Options{KeepDescriptors: true})
	r, err := git.Open(s, fs)
	CheckIfError(err)
	defer s.Close()

	blobMap := make(map[plumbing.Hash]BigFile)
	duplicateBlob := make(map[plumbing.Hash]bool)
	commitMap := make(map[plumbing.Hash]string)

	biter, _ := r.Branches()
	biter.ForEach(func(b *plumbing.Reference) error {
		citer, _ := r.Log(&git.LogOptions{From: b.Hash(), Since: &since, Until: &until})
		citer.ForEach(func(cc *object.Commit) error {
			commitMap[cc.Hash] = b.Name().String()
			return nil
		})
		return nil
	})
	fmt.Println("符合条件的commit数量：", len(commitMap))

	for ch, bn := range commitMap {
		commit, _ := r.CommitObject(ch)
		fs, _ := commit.Stats()
		for _, s := range fs {
			if s.Addition+s.Deletion > 0 {
				fmt.Println("commit stats with branch: ", bn, commit.Hash, s.Name, s.Addition, s.Deletion)
			}
		}

		//找到该commit所有文件，判断文件提交时间是否在更早的时间，如果是，则跳过
		fi, _ := commit.Files()
		for f, err := fi.Next(); err == nil; {
			if duplicateBlob[f.Hash] {
				f, err = fi.Next()
				continue
			} else {
				duplicateBlob[f.Hash] = true
			}
			var fileInOldCommit bool
			cutoffTime := since.Add(-1 * time.Nanosecond)
			commitWithOldFileIter, _ := r.Log(&git.LogOptions{From: commit.Hash, FileName: &f.Name, Order: git.LogOrderCommitterTime, Until: &cutoffTime})
			for commitWithOldFile, err := commitWithOldFileIter.Next(); err == nil; {
				oldCommitFileIter, _ := commitWithOldFile.Files()
				for oldfile, err := oldCommitFileIter.Next(); err == nil; {
					if oldfile.Hash == f.Hash {
						fileInOldCommit = true
						break
					}
					oldfile, err = oldCommitFileIter.Next()
				}
				if fileInOldCommit {
					break
				}
				commitWithOldFile, err = commitWithOldFileIter.Next()
			}
			if !fileInOldCommit {
				blobMap[f.Hash] = BigFile{bn, *commit, *f}
			}
			f, err = fi.Next()
		}
	}
	for _, f := range blobMap {
		fmt.Println(f.Branch, f.Commit.Hash, f.File.Name, f.File.Size)
	}
	fmt.Println(time.Since(timeStart))

}

func getCommitNodeIndex(r *git.Repository, fs billy.Filesystem) (commitgraph.CommitNodeIndex, io.ReadCloser) {
	file, err := fs.Open(path.Join("objects", "info", "commit-graph"))
	if err == nil {
		index, err := commitgraph_fmt.OpenFileIndex(file)
		if err == nil {
			return commitgraph.NewGraphCommitNodeIndex(index, r.Storer), file
		}
		file.Close()
	}

	return commitgraph.NewObjectCommitNodeIndex(r.Storer), nil
}

type commitAndPaths struct {
	commit commitgraph.CommitNode
	// Paths that are still on the branch represented by commit
	paths []string
	// Set of hashes for the paths
	hashes map[string]plumbing.Hash
}

func getCommitTree(c commitgraph.CommitNode, treePath string) (*object.Tree, error) {
	tree, err := c.Tree()
	if err != nil {
		return nil, err
	}

	// Optimize deep traversals by focusing only on the specific tree
	if treePath != "" {
		tree, err = tree.Tree(treePath)
		if err != nil {
			return nil, err
		}
	}

	return tree, nil
}

func getFullPath(treePath, path string) string {
	if treePath != "" {
		if path != "" {
			return treePath + "/" + path
		}
		return treePath
	}
	return path
}

func getFileHashes(c commitgraph.CommitNode, treePath string, paths []string) (map[string]plumbing.Hash, error) {
	tree, err := getCommitTree(c, treePath)
	if err == object.ErrDirectoryNotFound {
		// The whole tree didn't exist, so return empty map
		return make(map[string]plumbing.Hash), nil
	}
	if err != nil {
		return nil, err
	}

	hashes := make(map[string]plumbing.Hash)
	for _, path := range paths {
		if path != "" {
			entry, err := tree.FindEntry(path)
			if err == nil {
				hashes[path] = entry.Hash
			}
		} else {
			hashes[path] = tree.Hash
		}
	}

	return hashes, nil
}

func getLastCommitForPaths(c commitgraph.CommitNode, treePath string, paths []string) (map[string]*object.Commit, error) {
	// We do a tree traversal with nodes sorted by commit time
	heap := binaryheap.NewWith(func(a, b interface{}) int {
		if a.(*commitAndPaths).commit.CommitTime().Before(b.(*commitAndPaths).commit.CommitTime()) {
			return 1
		}
		return -1
	})

	resultNodes := make(map[string]commitgraph.CommitNode)
	initialHashes, err := getFileHashes(c, treePath, paths)
	if err != nil {
		return nil, err
	}

	// Start search from the root commit and with full set of paths
	heap.Push(&commitAndPaths{c, paths, initialHashes})

	for {
		cIn, ok := heap.Pop()
		if !ok {
			break
		}
		current := cIn.(*commitAndPaths)

		// Load the parent commits for the one we are currently examining
		numParents := current.commit.NumParents()
		var parents []commitgraph.CommitNode
		for i := 0; i < numParents; i++ {
			parent, err := current.commit.ParentNode(i)
			if err != nil {
				break
			}
			parents = append(parents, parent)
		}

		// Examine the current commit and set of interesting paths
		pathUnchanged := make([]bool, len(current.paths))
		parentHashes := make([]map[string]plumbing.Hash, len(parents))
		for j, parent := range parents {
			parentHashes[j], err = getFileHashes(parent, treePath, current.paths)
			if err != nil {
				break
			}

			for i, path := range current.paths {
				if parentHashes[j][path] == current.hashes[path] {
					pathUnchanged[i] = true
				}
			}
		}

		var remainingPaths []string
		for i, path := range current.paths {
			// The results could already contain some newer change for the same path,
			// so don't override that and bail out on the file early.
			if resultNodes[path] == nil {
				if pathUnchanged[i] {
					// The path existed with the same hash in at least one parent so it could
					// not have been changed in this commit directly.
					remainingPaths = append(remainingPaths, path)
				} else {
					// There are few possible cases how can we get here:
					// - The path didn't exist in any parent, so it must have been created by
					//   this commit.
					// - The path did exist in the parent commit, but the hash of the file has
					//   changed.
					// - We are looking at a merge commit and the hash of the file doesn't
					//   match any of the hashes being merged. This is more common for directories,
					//   but it can also happen if a file is changed through conflict resolution.
					resultNodes[path] = current.commit
				}
			}
		}

		if len(remainingPaths) > 0 {
			// Add the parent nodes along with remaining paths to the heap for further
			// processing.
			for j, parent := range parents {
				// Combine remainingPath with paths available on the parent branch
				// and make union of them
				remainingPathsForParent := make([]string, 0, len(remainingPaths))
				newRemainingPaths := make([]string, 0, len(remainingPaths))
				for _, path := range remainingPaths {
					if parentHashes[j][path] == current.hashes[path] {
						remainingPathsForParent = append(remainingPathsForParent, path)
					} else {
						newRemainingPaths = append(newRemainingPaths, path)
					}
				}

				if remainingPathsForParent != nil {
					heap.Push(&commitAndPaths{parent, remainingPathsForParent, parentHashes[j]})
				}

				if len(newRemainingPaths) == 0 {
					break
				} else {
					remainingPaths = newRemainingPaths
				}
			}
		}
	}

	// Post-processing
	result := make(map[string]*object.Commit)
	for path, commitNode := range resultNodes {
		var err error
		result[path], err = commitNode.Commit()
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}
