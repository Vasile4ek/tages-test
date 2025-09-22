package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	file_service "github.com/vasile4ek/tages-test/proto/gen"
)

func main() {
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := file_service.NewFileServiceClient(conn)
	basePath := "./uploads/"
	files, _ := os.ReadDir(basePath)
	buff := make(chan struct{}, 11)
	for _, file := range files {
		buff <- struct{}{}
		go func() {
			if err := uploadFile(client, basePath+file.Name()); err != nil {
				log.Printf("Upload failed: %v", err)
			}
			<-buff
		}()
	}

	// Пример загрузки файла
	if err := uploadFile(client, "./uploads/test.jpg"); err != nil {
		log.Printf("Upload failed: %v", err)
	}
	if err := uploadFile(client, "./uploads/test2.jpg"); err != nil {
		log.Printf("Upload failed: %v", err)
	}
	if err := uploadFile(client, "./uploads/test.bmp"); err != nil {
		log.Printf("Upload failed: %v", err)
	}
	// Пример получения списка файлов
	if err := listFiles(client); err != nil {
		log.Printf("List files failed: %v", err)
	}

	// Пример скачивания файла
	if err := downloadFile(client, "test.jpg", "downloads/test.jpg"); err != nil {
		log.Printf("Download failed: %v", err)
	}
}

func uploadFile(client file_service.FileServiceClient, filename string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	stream, err := client.UploadFile(context.Background())
	if err != nil {
		return fmt.Errorf("failed to create upload stream: %w", err)
	}

	// Отправляем информацию о файле
	if err := stream.Send(&file_service.UploadRequest{
		Data: &file_service.UploadRequest_Info{
			Info: &file_service.FileInfo{Filename: filename},
		},
	}); err != nil {
		return fmt.Errorf("failed to send file info: %w", err)
	}

	// Отправляем данные чанками
	chunkSize := 64 * 1024
	for i := 0; i < len(data); i += chunkSize {
		end := min(i+chunkSize, len(data))

		if err := stream.Send(&file_service.UploadRequest{
			Data: &file_service.UploadRequest_ChunkData{
				ChunkData: data[i:end],
			},
		}); err != nil {
			return fmt.Errorf("failed to send chunk: %w", err)
		}
	}

	response, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("failed to receive response: %w", err)
	}

	fmt.Printf("Upload successful: %s (size: %d bytes)\n", response.Filename, response.Size)
	return nil
}

func listFiles(client file_service.FileServiceClient) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	response, err := client.ListFiles(ctx, &file_service.ListRequest{})
	if err != nil {
		return fmt.Errorf("failed to list files: %w", err)
	}

	fmt.Println("Files list:")
	fmt.Println("Filename | Created At | Updated At | Size")
	fmt.Println("----------------------------------------")
	for _, file := range response.Files {
		fmt.Printf("%s | %s | %s | %d bytes\n",
			file.Filename,
			file.CreatedAt.AsTime().Format("2006-01-02 15:04:05"),
			file.UpdatedAt.AsTime().Format("2006-01-02 15:04:05"),
			file.Size)
	}

	return nil
}

func downloadFile(client file_service.FileServiceClient, remoteFilename, localFilename string) error {
	ctx := context.Background()
	req := &file_service.DownloadRequest{Filename: remoteFilename}

	stream, err := client.DownloadFile(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to create download stream: %w", err)
	}

	var fileData []byte
	var filename string

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to receive chunk: %w", err)
		}

		switch data := resp.Data.(type) {
		case *file_service.DownloadResponse_Info:
			filename = data.Info.Filename
		case *file_service.DownloadResponse_ChunkData:
			fileData = append(fileData, data.ChunkData...)
		}
	}

	if err := os.WriteFile(localFilename, fileData, 0644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	fmt.Printf("Download successful: %s (%d bytes)\n", localFilename, len(fileData))
	_ = filename

	return nil
}
