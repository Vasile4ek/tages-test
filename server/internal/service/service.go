package service

import (
	"context"
	"io"

	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	file_service "github.com/vasile4ek/tages-test/proto/gen"
	"github.com/vasile4ek/tages-test/server/internal/storage"
)

type FileService struct {
	file_service.UnimplementedFileServiceServer
	storage     *storage.FileStorage
	uploadSem   *semaphore.Weighted
	downloadSem *semaphore.Weighted
	listSem     *semaphore.Weighted
}

func NewFileService(storagePath string) (*FileService, error) {
	storage, err := storage.NewFileStorage(storagePath)
	if err != nil {
		return nil, err
	}

	return &FileService{
		storage:     storage,
		uploadSem:   semaphore.NewWeighted(10),  // 10 одновременных загрузок
		downloadSem: semaphore.NewWeighted(10),  // 10 одновременных скачиваний
		listSem:     semaphore.NewWeighted(100), // 100 одновременных запросов списка
	}, nil
}

func (s *FileService) UploadFile(stream file_service.FileService_UploadFileServer) error {
	if !s.uploadSem.TryAcquire(1) {
		return status.Errorf(codes.ResourceExhausted, "too many upload requests")
	}
	defer s.uploadSem.Release(1)

	var filename string
	var fileData []byte

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "failed to receive chunk: %v", err)
		}

		switch data := req.Data.(type) {
		case *file_service.UploadRequest_Info:
			filename = data.Info.Filename
			if filename == "" {
				return status.Errorf(codes.InvalidArgument, "filename is required")
			}
		case *file_service.UploadRequest_ChunkData:
			fileData = append(fileData, data.ChunkData...)
		}
	}

	if err := s.storage.SaveFile(filename, fileData); err != nil {
		return status.Errorf(codes.Internal, "failed to save file: %v", err)
	}

	return stream.SendAndClose(&file_service.UploadResponse{
		Filename: filename,
		Size:     uint32(len(fileData)),
		Message:  "File uploaded successfully",
	})
}

func (s *FileService) DownloadFile(req *file_service.DownloadRequest, stream file_service.FileService_DownloadFileServer) error {
	if !s.downloadSem.TryAcquire(1) {
		return status.Errorf(codes.ResourceExhausted, "too many download requests")
	}
	defer s.downloadSem.Release(1)

	if req.Filename == "" {
		return status.Errorf(codes.InvalidArgument, "filename is required")
	}

	if err := stream.Send(&file_service.DownloadResponse{
		Data: &file_service.DownloadResponse_Info{
			Info: &file_service.FileInfo{Filename: req.Filename},
		},
	}); err != nil {
		return status.Errorf(codes.Internal, "failed to send file info: %v", err)
	}

	data, err := s.storage.GetFile(req.Filename)
	if err != nil {
		return status.Errorf(codes.NotFound, "file not found: %v", err)
	}

	chunkSize := 64 * 1024
	for i := 0; i < len(data); i += chunkSize {
		end := min(i+chunkSize, len(data))

		if err := stream.Send(&file_service.DownloadResponse{
			Data: &file_service.DownloadResponse_ChunkData{
				ChunkData: data[i:end],
			},
		}); err != nil {
			return status.Errorf(codes.Internal, "failed to send chunk: %v", err)
		}
	}

	return nil
}

func (s *FileService) ListFiles(ctx context.Context, req *file_service.ListRequest) (*file_service.ListResponse, error) {
	if !s.listSem.TryAcquire(1) {
		return nil, status.Errorf(codes.ResourceExhausted, "too many list requests")
	}
	defer s.listSem.Release(1)

	files, err := s.storage.ListFiles()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list files: %v", err)
	}

	return &file_service.ListResponse{Files: files}, nil
}
