package testing

import (
	"errors"
	"io/fs"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
)

type DirFixture struct {
	Dir string
}

func SetupDir() *DirFixture {
	dir, err := ioutil.TempDir("", "test")
	if err != nil {
		panic(err)
	}
	return &DirFixture{
		Dir: dir,
	}
}

func (f *DirFixture) TearDown() {
	os.RemoveAll(f.Dir)
}

func (f *DirFixture) Path(fileName string) string {
	if fileName == "." {
		return f.Dir
	}
	return path.Join(f.Dir, fileName)
}

func (f *DirFixture) Touch(p string) string {
	return f.WriteFile(p, "")
}

func (f *DirFixture) TouchAll(all []string) {
	for _, p := range all {
		f.WriteFile(p, "")
	}
}

func (f *DirFixture) SymLink(from string, to string) {
	f.MakeDir(path.Dir(to))
	err := os.Symlink(f.Path(from), f.Path(to))
	if err != nil {
		panic(err)
	}
}

func (f *DirFixture) WriteFile(p string, content string) string {
	f.MakeDir(path.Dir(p))
	err := ioutil.WriteFile(f.Path(p), []byte(content), 0777)
	if err != nil {
		panic(err)
	}
	return f.Path(p)
}

func (f *DirFixture) MakeDir(p string) string {
	err := os.MkdirAll(f.Path(p), 0777)
	if err != nil {
		panic(err)
	}
	return f.Path(p)
}

func (f *DirFixture) IsSymlink(p string) bool {
	info, err := os.Lstat(f.Path(p))
	if err != nil {
		panic(err)
	}
	return (info.Mode() & os.ModeSymlink) != 0
}

func (f *DirFixture) Exists(p string) bool {
	_, err := os.Stat(f.Path(p))
	if err == nil {
		return true
	}
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	panic(err)
}

func (f *DirFixture) FileContentString(p string) string {
	bytes, err := ioutil.ReadFile(f.Path(p))
	if err != nil {
		return "<error>"
	}
	return string(bytes)
}

func (f *DirFixture) FilesIn(p string) []string {
	basePath := f.Path(p)
	files := []string{}
	err := filepath.WalkDir(basePath, func(filePath string, info fs.DirEntry, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		relPath, err := filepath.Rel(basePath, filePath)
		if err != nil {
			return err
		}
		if relPath != "." {
			files = append(files, relPath)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	return files
}
