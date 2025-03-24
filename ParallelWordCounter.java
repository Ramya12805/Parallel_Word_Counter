import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.*;
import java.util.stream.Collectors;

public class ParallelWordCounter {
    private static final int NUM_THREADS = Runtime.getRuntime().availableProcessors();
    private static final int BATCH_SIZE = 5;
    private static final ExecutorService fileThreadPool = Executors.newFixedThreadPool(NUM_THREADS);
    private static final ExecutorService contentThreadPool = Executors.newFixedThreadPool(NUM_THREADS);

    static class TextStats {
        String name;
        String threadName;
        ConcurrentHashMap<String, Integer> wordCounts = new ConcurrentHashMap<>();
        int lineCount = 0;
        int charCount = 0;
        int wordCount = 0;
        List<String> fileContents = new ArrayList<>();

        public TextStats(String name, String threadName) {
            this.name = name;
            this.threadName = threadName;
        }

        public synchronized void merge(TextStats other) {
            mergeWordCounts(this.wordCounts, other.wordCounts);
            this.lineCount += other.lineCount;
            this.charCount += other.charCount;
            this.wordCount += other.wordCount;
            this.fileContents.addAll(other.fileContents);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        if (args.length < 1) {
            System.out.println("Usage: java ParallelTextAnalyzer <directory_path>");
            return;
        }

        Path folderPath = Paths.get(args[0]);
        if (!Files.exists(folderPath) || !Files.isDirectory(folderPath)) {
            System.out.println("Error: Directory does not exist: " + folderPath);
            return;
        }

        List<Path> files = Files.list(folderPath)
                .filter(Files::isRegularFile)
                .collect(Collectors.toList());

        if (files.isEmpty()) {
            System.out.println("No files found in the directory: " + folderPath);
            return;
        }

        int batchCount = (int) Math.ceil((double) files.size() / BATCH_SIZE);
        System.out.println("\n=====================================");
        System.out.println("Total files: " + files.size());
        System.out.println("Processing in " + batchCount + " batch(es) in order...");
        System.out.println("=====================================\n");

        TextStats finalStats = new TextStats("All Files", "N/A");
        for (int batchNum = 0; batchNum < batchCount; batchNum++) {
            TextStats batchStats = processBatch(files, batchNum);
            finalStats.merge(batchStats);

            synchronized (System.out) {
                System.out.println("\n=====================================");
                displayStats(batchStats, "BATCH " + (batchNum + 1) + " SUMMARY");
            }
        }

        System.out.println("\n=====================================");
        displayStats(finalStats, "FINAL RESULTS (ALL FILES)");

        fileThreadPool.shutdown();
        contentThreadPool.shutdown();
    }

    private static TextStats processBatch(List<Path> allFiles, int batchNum) throws InterruptedException, ExecutionException {
        int start = batchNum * BATCH_SIZE;
        int end = Math.min(start + BATCH_SIZE, allFiles.size());
        List<Path> batchFiles = allFiles.subList(start, end);

        synchronized (System.out) {
            System.out.println("\n-------------------------------------");
            System.out.println("BATCH " + (batchNum + 1) + " STARTING...");
            System.out.println("Processing files " + (start + 1) + " to " + end + " of " + allFiles.size());
            System.out.println("-------------------------------------");
        }

        List<Future<TextStats>> fileFutures = new ArrayList<>();
        ConcurrentHashMap<String, String> threadAssignments = new ConcurrentHashMap<>();

        for (Path file : batchFiles) {
            fileFutures.add(fileThreadPool.submit(() -> {
                TextStats stats = processFile(file);
                threadAssignments.put(stats.name, stats.threadName);
                return stats;
            }));
        }

        TextStats batchStats = new TextStats("Batch " + (batchNum + 1), "N/A");

        // Collect thread assignments before printing
        List<TextStats> fileStatsList = new ArrayList<>();
        for (Future<TextStats> future : fileFutures) {
            fileStatsList.add(future.get());
        }

        synchronized (System.out) {
            System.out.println("\nTHREAD ASSIGNMENTS (Batch " + (batchNum + 1) + "):");
            threadAssignments.forEach((fileName, threadName) ->
                System.out.printf("Thread %-20s -> File: %s%n", threadName, fileName)
            );

            for (TextStats fileStats : fileStatsList) {
                batchStats.merge(fileStats);
                displayStats(fileStats, "FILE SUMMARY: " + fileStats.name);
            }
        }

        return batchStats;
    }

    private static TextStats processFile(Path file) throws IOException, InterruptedException, ExecutionException {
        String fileName = file.getFileName().toString();
        String threadName = Thread.currentThread().getName();
        TextStats fileStats = new TextStats(fileName, threadName);
        List<Future<Void>> chunkFutures = new ArrayList<>();

        try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            List<String> chunk = new ArrayList<>();
            String line;
            int chunkSize = 100;

            while ((line = reader.readLine()) != null) {
                fileStats.fileContents.add(line);
                chunk.add(line);

                if (chunk.size() >= chunkSize) {
                    List<String> chunkCopy = new ArrayList<>(chunk);
                    chunkFutures.add(contentThreadPool.submit(() -> {
                        analyzeTextChunk(chunkCopy, fileStats);
                        return null;
                    }));
                    chunk.clear();
                }
            }

            if (!chunk.isEmpty()) {
                chunkFutures.add(contentThreadPool.submit(() -> {
                    analyzeTextChunk(chunk, fileStats);
                    return null;
                }));
            }
        }

        for (Future<Void> future : chunkFutures) {
            future.get();
        }

        return fileStats;
    }

    private static void analyzeTextChunk(List<String> lines, TextStats stats) {
        Pattern pattern = Pattern.compile("[\\p{L}\\p{N}']+");

        for (String line : lines) {
            stats.lineCount++;
            stats.charCount += line.length();

            Matcher matcher = pattern.matcher(line.toLowerCase());
            Map<String, Integer> localWordCounts = new HashMap<>();
            int localWordCount = 0;

            while (matcher.find()) {
                String word = matcher.group();
                localWordCount++;
                localWordCounts.merge(word, 1, Integer::sum);
            }

            stats.wordCount += localWordCount;
            mergeWordCounts(stats.wordCounts, localWordCounts);
        }
    }

    private static void mergeWordCounts(ConcurrentHashMap<String, Integer> global, Map<String, Integer> local) {
        local.forEach((word, count) ->
                global.compute(word, (k, v) -> (v == null) ? count : v + count)
        );
    }

    private static void displayStats(TextStats stats, String label) {
        System.out.println("\n=== " + label + " ===");
        System.out.printf("Line Count: %d\n", stats.lineCount);
        System.out.printf("Character Count: %d\n", stats.charCount);
        System.out.printf("Word Count: %d\n", stats.wordCount);
        System.out.printf("Unique Word Count: %d\n", stats.wordCounts.size());

        System.out.println("\nTop 5 Most Frequent Words:");
        stats.wordCounts.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue(Comparator.reverseOrder()))
                .limit(5)
                .forEach(entry -> System.out.printf("%-10s : %d\n", entry.getKey(), entry.getValue()));

        System.out.println("\n---------------------------------------");
    }
}
