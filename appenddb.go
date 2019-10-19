package appenddb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type AppendDbV1 struct {
	Path     string
	fp_index *os.File
	fp_data  *os.File
}

func NewV1(path string) (*AppendDbV1, error) {
	os.MkdirAll(path, 0700)
	fp_index, err := os.OpenFile(filepath.Join(path, "index.i64"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return &AppendDbV1{}, err
	}
	fp_data, err := os.OpenFile(filepath.Join(path, "data.txt"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return &AppendDbV1{}, err
	}
	return &AppendDbV1{
		Path:     path,
		fp_index: fp_index,
		fp_data:  fp_data,
	}, nil
}

func (ad *AppendDbV1) Close() error {
	// todo: improve.
	err_index := ad.fp_index.Close()
	err_data := ad.fp_data.Close()
	if err_index != nil {
		return err_index
	}
	if err_data != nil {
		return err_data
	}
	return nil
}

func (ad *AppendDbV1) Len() (int64, error) {
	fi, err := ad.fp_index.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size() / 8, nil
}

func (ad *AppendDbV1) Get(i int64) (string, error) {
	ib1, err := ad.ib1Of(i)
	if err != nil {
		return "", err
	}
	ib2, err := ad.ib2Of(i)
	if err != nil {
		return "", err
	}
	_, err = ad.fp_data.Seek(ib1, 0)
	if err != nil {
		return "", err
	}
	n_should_read := ib2 - ib1
	buf := make([]byte, n_should_read)
	n, err := ad.fp_data.Read(buf)
	if err != nil {
		return "", err
	}
	if int64(n) != n_should_read {
		return "", fmt.Errorf("Not enough bytes were read from %+v", ad)
	}
	return string(buf), nil
}

func (ad *AppendDbV1) Append(t string) error {
	buf := []byte(t)
	l, err := ad.Len()
	if err != nil {
		return err
	}
	ib1, err := ad.ib1Of(l)
	if err != nil {
		return err
	}
	_, err = ad.fp_data.Seek(ib1, 0)
	if err != nil {
		return err
	}
	dib, err := ad.fp_data.Write(buf)
	if err != nil {
		return err
	}
	ib2 := ib1 + int64(dib)
	_, err = ad.fp_index.Seek(l*8, 0)
	if err != nil {
		return err
	}
	err = binary.Write(ad.fp_index, binary.LittleEndian, &ib2)
	if err != nil {
		msg := err.Error()
		err_trunc := ad.fp_index.Truncate(l * 8)
		if err_trunc != nil {
			msg = msg + "\n    " + err_trunc.Error()
		}
		return errors.New(msg)
	}
	err = ad.Sync()
	if err != nil {
		return err
	}
	return nil
}

func (ad *AppendDbV1) Sync() error {
	err := ad.fp_data.Sync()
	if err != nil {
		return err
	}
	err = ad.fp_index.Sync()
	if err != nil {
		return err
	}
	return nil
}

func (ad *AppendDbV1) ib1Of(i int64) (int64, error) {
	if i == 0 {
		return 0, nil
	} else {
		ib, err := ad.ibOf(i - 1)
		if err != nil {
			return 0, err
		}
		return ib, nil
	}
}

func (ad *AppendDbV1) ib2Of(i int64) (int64, error) {
	ib, err := ad.ibOf(i)
	if err != nil {
		return 0, err
	}
	return ib, nil
}

func (ad *AppendDbV1) ibOf(i int64) (int64, error) {
	_, err := ad.fp_index.Seek(i*8, 0)
	if err != nil {
		return 0, err
	}
	var ib int64
	err = binary.Read(ad.fp_index, binary.LittleEndian, &ib)
	if err != nil {
		return 0, err
	}
	return ib, nil
}
