package main

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
)

func main() {
	lambdaDir := "lambda/upload_dashboard"
	bootstrapPath := filepath.Join(lambdaDir, "bootstrap")
	mainGoPath := filepath.Join(lambdaDir, "main.go")
	zipPath := filepath.Join(lambdaDir, "lambda.zip")

	cmd := exec.Command("go", "build", "-o", bootstrapPath, mainGoPath)
	cmd.Env = append(os.Environ(), "GOOS=linux", "GOARCH=arm64")

	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Printf("Error building Lambda function: %v\n%s\n", err, output)
		os.Exit(1)
	}

	if err := createZip(zipPath, bootstrapPath); err != nil {
		fmt.Printf("Error creating zip file: %v\n", err)
		os.Exit(1)
	}

	if err := os.Remove(bootstrapPath); err != nil {
		fmt.Printf("Error removing bootstrap file: %v\n", err)
		os.Exit(1)
	}
}

// create Lambda function zip
func createZip(zipPath, sourcePath string) error {
	zipFile, err := os.Create(zipPath)
	if err != nil {
		return err
	}
	defer zipFile.Close()

	zipWriter := zip.NewWriter(zipFile)
	defer zipWriter.Close()

	sourceFile, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	info, err := sourceFile.Stat()
	if err != nil {
		return err
	}

	header, err := zip.FileInfoHeader(info)
	if err != nil {
		return err
	}

	header.Name = filepath.Base(sourcePath)

	writer, err := zipWriter.CreateHeader(header)
	if err != nil {
		return err
	}

	_, err = io.Copy(writer, sourceFile)
	return err
}
