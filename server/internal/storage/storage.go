package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	file_service "github.com/vasile4ek/file-service-proto/gen"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type FileStorage struct {
	basePath string
	mu       sync.RWMutex
}

func NewFileStorage(basePath string) (*FileStorage, error) {
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage directory: %w", err)
	}
	return &FileStorage{basePath: basePath}, nil
}

func (s *FileStorage) SaveFile(filename string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	filePath := filepath.Join(s.basePath, filename)
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to save file: %w", err)
	}
	return nil
}

func (s *FileStorage) GetFile(filename string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	filePath := filepath.Join(s.basePath, filename)
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}
	return data, nil
}

func (s *FileStorage) ListFiles() ([]*file_service.FileMetadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	files, err := os.ReadDir(s.basePath)
	if err != nil {
		return nil, fmt.Errorf("failed to list files: %w", err)
	}

	var metadata []*file_service.FileMetadata
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		info, err := file.Info()
		if err != nil {
			continue
		}

		metadata = append(metadata, &file_service.FileMetadata{
			Filename:  file.Name(),
			CreatedAt: timestamppb.New(info.ModTime()),
			UpdatedAt: timestamppb.New(info.ModTime()),
			Size:      uint32(info.Size()),
		})
	}

	return metadata, nil
}

func (s *FileStorage) FileExists(filename string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	filePath := filepath.Join(s.basePath, filename)
	_, err := os.Stat(filePath)
	return err == nil
}
