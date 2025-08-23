#!/usr/bin/env node
/**
 * Script to compare IDs from 20250102_centralidade_local_11181.csv with other CSV files
 * to check if all IDs are present across all files.
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import csv from 'csv-parser';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function loadIdsFromCsv(filePath, idColumnName) {
    return new Promise((resolve, reject) => {
        const ids = new Set();
        let isFirstRow = true;
        
        const results = [];
        
        fs.createReadStream(filePath)
            .pipe(csv({ separator: ';' }))
            .on('data', (row) => {
                if (isFirstRow) {
                    // Check if the column exists
                    if (!(idColumnName in row)) {
                        reject(new Error(`Column '${idColumnName}' not found in ${path.basename(filePath)}`));
                        return;
                    }
                    isFirstRow = false;
                }
                
                const id = row[idColumnName];
                if (id && id.toString().trim() !== '') {
                    ids.add(id.toString().trim());
                }
            })
            .on('end', () => {
                console.log(`  Loaded ${ids.size} IDs from ${path.basename(filePath)}`);
                resolve(ids);
            })
            .on('error', (error) => {
                reject(error);
            });
    });
}

function getCsvFiles(dataDir) {
    try {
        const files = fs.readdirSync(dataDir)
            .filter(file => file.endsWith('.csv'))
            .filter(file => file !== '20250102_centralidade_local_11181.csv')
            .sort();
        return files;
    } catch (error) {
        throw new Error(`Error reading directory ${dataDir}: ${error.message}`);
    }
}

function getColumnNames(filePath) {
    return new Promise((resolve, reject) => {
        const columns = [];
        let isFirstRow = true;
        
        fs.createReadStream(filePath)
            .pipe(csv({ separator: ';' }))
            .on('data', (row) => {
                if (isFirstRow) {
                    columns.push(...Object.keys(row));
                    isFirstRow = false;
                    // Stop reading after first row
                    
                }
            })
            .on('end', () => {
                resolve(columns);
            })
            .on('error', (error) => {
                reject(error);
            });
    });
}

async function main() {
    const dataDir = path.join(__dirname, 'data');
    
    // Check if data directory exists
    if (!fs.existsSync(dataDir)) {
        console.error("Error: 'data' directory not found!");
        process.exit(1);
    }
    
    // Reference file
    const referenceFile = path.join(dataDir, '20250102_centralidade_local_11181.csv');
    if (!fs.existsSync(referenceFile)) {
        console.error("Error: Reference file '20250102_centralidade_local_11181.csv' not found!");
        process.exit(1);
    }
    
    console.log("=== CSV ID Comparison Script ===");
    console.log(`Reference file: ${path.basename(referenceFile)}`);
    console.log();
    
    try {
        // Load reference IDs (first column: ID_CENTRALIDADE_LOCAL)
        console.log("Loading reference IDs...");
        const referenceIds = await loadIdsFromCsv(referenceFile, 'ID_CENTRALIDADE_LOCAL');
        
        if (referenceIds.size === 0) {
            console.error("Error: No reference IDs found!");
            process.exit(1);
        }
        
        console.log(`Total reference IDs: ${referenceIds.size}`);
        console.log();
        
        // Get all other CSV files
        const csvFiles = getCsvFiles(dataDir);
        
        console.log(`Found ${csvFiles.length} other CSV files to compare:`);
        csvFiles.forEach(file => {
            console.log(`  - ${file}`);
        });
        console.log();
        
        // Compare IDs with each file
        const results = {};
        
        for (const csvFile of csvFiles) {
            const filePath = path.join(dataDir, csvFile);
            console.log(`Processing ${csvFile}...`);
            
            try {
                // Get column names to determine which column to use for IDs
                const columns = await getColumnNames(filePath);
                let idColumn = 'ID_BASE_TRECHO';
                
                if (!columns.includes(idColumn)) {
                    // Use first column if ID_BASE_TRECHO is not found
                    idColumn = columns[0];
                    console.log(`  Using first column '${idColumn}' (ID_BASE_TRECHO not found)`);
                }
                
                const fileIds = await loadIdsFromCsv(filePath, idColumn);
                
                // Find intersection with reference IDs
                const commonIds = new Set([...referenceIds].filter(id => fileIds.has(id)));
                const missingIds = new Set([...referenceIds].filter(id => !fileIds.has(id)));
                
                results[csvFile] = {
                    totalIds: fileIds.size,
                    commonIds: commonIds.size,
                    missingIds: missingIds.size,
                    missingList: Array.from(missingIds).slice(0, 10) // Show first 10 missing IDs
                };
                
                console.log(`  Common IDs: ${commonIds.size}`);
                console.log(`  Missing IDs: ${missingIds.size}`);
                
                if (missingIds.size > 0) {
                    console.log(`  First 10 missing IDs: ${Array.from(missingIds).slice(0, 10)}`);
                }
                
            } catch (error) {
                console.error(`  Error processing ${csvFile}: ${error.message}`);
                results[csvFile] = {
                    error: error.message
                };
            }
            
            console.log();
        }
        
        // Summary report
        console.log("=== SUMMARY REPORT ===");
        console.log(`Reference file: ${path.basename(referenceFile)}`);
        console.log(`Total reference IDs: ${referenceIds.size}`);
        console.log();
        
        const allMissing = new Set();
        
        for (const [filename, result] of Object.entries(results)) {
            if (result.error) {
                console.log(`${filename}: ERROR - ${result.error}`);
            } else {
                console.log(`${filename}:`);
                console.log(`  Total IDs: ${result.totalIds}`);
                console.log(`  Common with reference: ${result.commonIds}`);
                console.log(`  Missing from reference: ${result.missingIds}`);
                
                if (result.missingIds > 0) {
                    console.log(`  Missing IDs: ${result.missingList}`);
                    result.missingList.forEach(id => allMissing.add(id));
                }
            }
            console.log();
        }
        
        // Overall assessment
        console.log("=== OVERALL ASSESSMENT ===");
        const filesWithErrors = Object.values(results).filter(r => r.error).length;
        const filesProcessed = Object.keys(results).length - filesWithErrors;
        
        if (filesProcessed === 0) {
            console.log("No files were successfully processed.");
        } else {
            const filesWithMissing = Object.values(results).filter(r => !r.error && r.missingIds > 0).length;
            
            if (filesWithMissing === 0) {
                console.log("✅ All reference IDs are present in all CSV files!");
            } else {
                console.log(`⚠️  ${filesWithMissing} out of ${filesProcessed} files are missing some reference IDs.`);
            }
            
            if (filesWithErrors > 0) {
                console.log(`❌ ${filesWithErrors} files had processing errors.`);
            }
        }
        
    } catch (error) {
        console.error(`Fatal error: ${error.message}`);
        process.exit(1);
    }
}

// Run the script
main().catch(error => {
    console.error(`Unhandled error: ${error.message}`);
    process.exit(1);
}); 
