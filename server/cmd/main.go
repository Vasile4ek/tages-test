package main

import (
	"log"
	"net"
	"os"

	"google.golang.org/grpc"

	file_service "github.com/vasile4ek/tages-test/proto/gen"
	"github.com/vasile4ek/tages-test/server/internal/service"
)

func main() {
	// Создаем папку для хранения файлов
	storagePath := "./uploads"
	if err := os.MkdirAll(storagePath, 0755); err != nil {
		log.Fatalf("Failed to create storage directory: %v", err)
	}

	// Создаем сервис
	fileService, err := service.NewFileService(storagePath)
	if err != nil {
		log.Fatalf("Failed to create file service: %v", err)
	}

	// Запускаем gRPC сервер
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	file_service.RegisterFileServiceServer(grpcServer, fileService)

	log.Printf("Server started on port 50051")
	log.Printf("Storage directory: %s", storagePath)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
