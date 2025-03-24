# Parallel Word Counter in Java 🧠⚡

This project is a **multithreaded Java application** designed to efficiently analyze a large set of text files using parallel processing. It counts the number of lines, words, characters, and also generates a frequency map of words across all files using thread pools.

---

## 🚀 Features

- ✅ Process multiple files **in parallel** using `ExecutorService`
- ✅ Chunk-wise text analysis for **faster processing**
- ✅ Thread-safe word frequency computation using `ConcurrentHashMap`
- ✅ Displays:
  - Total lines
  - Total words
  - Total characters
  - Unique word count
  - Top 5 most frequent words

---

## ⚙️ How It Works

1. **Reads text files** from a given directory.
2. **Splits files into batches** (default: 5 files per batch).
3. Each file is **processed in a separate thread**.
4. Files are split into **chunks of 100 lines**.
5. Chunks are analyzed concurrently and results are merged.
6. **Final summary** is printed after processing all files.

---

## 🧠 Technologies Used

- Java 8+
- Multithreading with `ExecutorService`
- `ConcurrentHashMap` for thread-safe operations
- Regular expressions for text parsing

---

## 📁 Input Files

All sample input files can be found in the `input_files.zip` folder. You can add your own `.txt` files to test the performance.

---

## 📜 Documentation

Detailed explanation of the code logic, flowchart, and viva questions are available in `Case Study Opt.docx`.

---
