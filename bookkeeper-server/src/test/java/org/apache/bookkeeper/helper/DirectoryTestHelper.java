package org.apache.bookkeeper.helper;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public enum DirectoryTestHelper {
    DIR_WITH_FILE {
        @Override
        public File[] fetchDirectories() {
            return generateTemporaryDirectoriesWithFiles(3);
        }
        @Override
        public String fetchGcLogMetadataPath() {
            return generateTemporaryDirectoryWithFile();
        }
    },
    DIR_WITH_SUBDIR_AND_FILE {
        @Override
        public File[] fetchDirectories() {
            return generateDirectoriesWithSubdirsAndFiles(3);
        }
        @Override
        public String fetchGcLogMetadataPath() {
            return generateTemporaryDirectoryWithSubdirAndFile();
        }
    },
    DIR_WITH_LOCKED_SUBDIR {
        @Override
        public File[] fetchDirectories() {
            return generateDirectoriesWithLockedSubdirs(3);
        }
        @Override
        public String fetchGcLogMetadataPath() {
            return generateTemporaryDirectoryWithLockedSubdir();
        }
    },
    DIR_WITH_LOCKED_FILE {
        @Override
        public File[] fetchDirectories() {
            return generateDirectoriesWithLockedFiles(3);
        }
        @Override
        public String fetchGcLogMetadataPath() {
            return generateTemporaryDirectoryWithLockedFile();
        }
    },
    DIR_WITH_LOCKED_EMPTY_SUBDIR {
        @Override
        public File[] fetchDirectories() {
            return generateDirectoryWithLockedEmptySubdir(3);
        }
        @Override
        public String fetchGcLogMetadataPath() {
            return generateTemporaryDirectoryWithLockedEmptySubdir();
        }

    },
    NON_EXISTENT_DIRS {
        @Override
        public File[] fetchDirectories() {
            return generateNonExistentDirectories(3);
        }
        @Override
        public String fetchGcLogMetadataPath() {
            return "nonexistentGcEntryLogMetadataPath";
        }
    },
    EMPTY_LIST {
        @Override
        public File[] fetchDirectories() {
            return new File[0];
        }
        @Override
        public String fetchGcLogMetadataPath() {
            return "";
        }
    },
    NULL {
        @Override
        public File[] fetchDirectories() {
            return null;
        }
        @Override
        public String fetchGcLogMetadataPath() {
            return null;
        }
    };

    public abstract File[] fetchDirectories();
    public abstract String fetchGcLogMetadataPath();

    private static File[] generateTemporaryDirectoriesWithFiles(int count) {
        File[] dirs = new File[count];
        for (int i = 0; i < count; i++) {
            try {
                File dir = Files.createTempDirectory("tempDirWithFile" + i).toFile();
                new File(dir, "file.txt").createNewFile();
                dirs[i] = dir;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return dirs;
    }

    private static String generateTemporaryDirectoryWithFile() {
        try {
            File dir = Files.createTempDirectory("tempGcPathWithFile").toFile();
            new File(dir, "file.txt").createNewFile();
            return dir.getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static File[] generateDirectoriesWithSubdirsAndFiles(int count) {
        File[] dirs = new File[count];
        for (int i = 0; i < count; i++) {
            try {
                File dir = Files.createTempDirectory("tempDirWithSubdir" + i).toFile();
                File subDir = new File(dir, "subdir");
                subDir.mkdir();
                new File(subDir, "file.txt").createNewFile();
                dirs[i] = dir;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return dirs;
    }

    private static String generateTemporaryDirectoryWithSubdirAndFile() {
        try {
            File dir = Files.createTempDirectory("tempGcPathWithSubdir").toFile();
            File subDir = new File(dir, "subdir");
            subDir.mkdir();
            new File(subDir, "file.txt").createNewFile();
            return dir.getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static File[] generateDirectoriesWithLockedSubdirs(int count) {
        File[] dirs = new File[count];
        for (int i = 0; i < count; i++) {
            try {
                File dir = Files.createTempDirectory("tempDirWithNonRemovableSubdir" + i).toFile();
                File subDir = new File(dir, "subdir");
                subDir.mkdir();
                if (!subDir.setWritable(false)) {
                    throw new RuntimeException("Failed to make directory non-removable");
                }
                dirs[i] = dir;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return dirs;
    }

    private static String generateTemporaryDirectoryWithLockedSubdir() {
        try {
            File dir = Files.createTempDirectory("tempDirWithNonRemovableSubdir").toFile();

            File subDir = new File(dir, "nonRemovableSubdir");
            if (!subDir.mkdir()) {
                throw new RuntimeException("Failed to create subdirectory: " + subDir.getAbsolutePath());
            }

            if (!subDir.setWritable(false, false) || !subDir.setExecutable(false, false) || !subDir.setReadable(true, false)) {
                throw new RuntimeException("Failed to make subdirectory non-removable: " + subDir.getAbsolutePath());
            }

            if (!dir.setWritable(false, false)) {
                throw new RuntimeException("Failed to make parent directory non-writable: " + dir.getAbsolutePath());
            }

            return dir.getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException("Failed to create temporary directory with non-removable subdirectory", e);
        }
    }

    private static File[] generateNonExistentDirectories(int count) {
        File[] dirs = new File[count];
        for (int i = 0; i < count; i++) {
            dirs[i] = new File("nonexistentDir" + i);
        }
        return dirs;
    }

    private static File[] generateDirectoriesWithLockedFiles(int count) {
        File[] dirs = new File[count];
        for (int i = 0; i < count; i++) {
            try {
                File dir = Files.createTempDirectory("tempDirWithNonRemovableFile" + i).toFile();
                File nonRemovableFile = new File(dir, "nonRemovableFile.txt");
                if (!nonRemovableFile.createNewFile()) {
                    throw new RuntimeException("Failed to create file: " + nonRemovableFile.getAbsolutePath());
                }
                if (!nonRemovableFile.setWritable(false, false) || !nonRemovableFile.setReadable(false, false)) {
                    throw new RuntimeException("Failed to make file non-removable: " + nonRemovableFile.getAbsolutePath());
                }
                if (!dir.setWritable(false, false)) {
                    throw new RuntimeException("Failed to make directory non-writable: " + dir.getAbsolutePath());
                }

                dirs[i] = dir;
            } catch (IOException e) {
                throw new RuntimeException("Failed to create directory with non-removable file", e);
            }
        }
        return dirs;
    }

    private static String generateTemporaryDirectoryWithLockedFile() {
        try {
            File dir = Files.createTempDirectory("tempDirWithNonRemovableFile").toFile();
            File nonRemovableFile = new File(dir, "nonRemovableFile.txt");
            if (!nonRemovableFile.createNewFile()) {
                throw new RuntimeException("Failed to create file: " + nonRemovableFile.getAbsolutePath());
            }

            if (!nonRemovableFile.setWritable(false, false) || !nonRemovableFile.setReadable(false, false)) {
                throw new RuntimeException("Failed to make file non-removable: " + nonRemovableFile.getAbsolutePath());
            }

            if (!dir.setWritable(false, false)) {
                throw new RuntimeException("Failed to make parent directory non-writable: " + dir.getAbsolutePath());
            }

            return dir.getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException("Failed to create temporary directory with non-removable file", e);
        }
    }

    private static File[] generateDirectoryWithLockedEmptySubdir(int count) {
        File[] dirs = new File[count];
        for (int i = 0; i < count; i++) {
            try {
                File dir = Files.createTempDirectory("tempDirWithNonRemovableEmptySubdir" + i).toFile();
                File subDir = new File(dir, "nonRemovableSubdir");
                if (!subDir.mkdir()) {
                    throw new RuntimeException("Failed to create subdirectory: " + subDir.getAbsolutePath());
                }

                if (!subDir.setWritable(false, false) || !subDir.setExecutable(false, false) || !subDir.setReadable(true, false)) {
                    throw new RuntimeException("Failed to make subdirectory non-removable: " + subDir.getAbsolutePath());
                }

                if (!dir.setWritable(false, false)) {
                    throw new RuntimeException("Failed to make parent directory non-writable: " + dir.getAbsolutePath());
                }

                dirs[i] = dir;
            } catch (IOException e) {
                throw new RuntimeException("Failed to create directory with non-removable empty subdirectory", e);
            }
        }
        return dirs;
    }

    private static String generateTemporaryDirectoryWithLockedEmptySubdir() {
        try {
            File dir = Files.createTempDirectory("tempDirWithNonRemovableEmptySubdir").toFile();

            File subDir = new File(dir, "nonRemovableEmptySubdir");
            if (!subDir.mkdir()) {
                throw new RuntimeException("Failed to create subdirectory: " + subDir.getAbsolutePath());
            }

            if (!subDir.setWritable(false, false) || !subDir.setExecutable(false, false) || !subDir.setReadable(true, false)) {
                throw new RuntimeException("Failed to make subdirectory non-removable: " + subDir.getAbsolutePath());
            }
            if (!dir.setWritable(false, false)) {
                throw new RuntimeException("Failed to make parent directory non-writable: " + dir.getAbsolutePath());
            }

            return dir.getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException("Failed to create temporary directory with non-removable empty subdirectory", e);
        }
    }
}