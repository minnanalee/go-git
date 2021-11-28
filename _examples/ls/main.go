package main

import (
	"fmt"
	"github.com/go-git/go-git/v5"
	. "github.com/go-git/go-git/v5/_examples"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/cache"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"time"

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
				fmt.Println("commit stats with branch:  ", bn, commit.Hash, s.Name, "\tadded: ", s.Addition, "\tdeleted: ", s.Deletion)
			}
		}
		firstParent, err := commit.Parents().Next()
		if err != nil {
			fmt.Println("1st ancestor:", commit.Hash.String())
			continue
		}
		patch, _ := firstParent.Patch(commit)
		var pc, pd []string //path added/changed, deleted
		for _, f := range patch.FilePatches() {
			from, to := f.Files()
			if from != nil && to != nil {
				pc = append(pc, from.Path())
			} else if to == nil {
				pd = append(pd, from.Path()) //deleted
			} else if from == nil {
				pc = append(pc, to.Path()) //added
			}
		}

		for _, p := range pc {
			f, err := commit.File(p)
			if err == nil && f != nil {
				fmt.Println("size info：", bn, commit.Hash, f.Name, f.Size)
			} else {
				fmt.Println("err size: ", f, err)
			}
		}
	}

	fmt.Println(time.Since(timeStart))

}
