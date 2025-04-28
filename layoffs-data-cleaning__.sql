-- SQL Project - Data Cleaning

-- Step 1: Create a staging table to work with the raw data
CREATE TABLE world_layoffso.layoffs_staging 
LIKE world_layoffso.layoffs;

INSERT INTO world_layoffso.layoffs_staging 
SELECT * FROM world_layoffso.layoffs;

-- Step 2: Check for duplicates
SELECT company, industry, total_laid_off, `date`,
       ROW_NUMBER() OVER (
           PARTITION BY company, industry, total_laid_off, `date`
           ORDER BY company
       ) AS row_num
FROM world_layoffso.layoffs_staging;

-- Step 3: Identify real duplicates (remove duplicates where row_num > 1)
SELECT *
FROM (
    SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
           ROW_NUMBER() OVER (
               PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
           ) AS row_num
    FROM world_layoffso.layoffs_staging
) duplicates
WHERE row_num > 1;

-- Step 4: Remove duplicates by creating a new column for row numbers and then deleting rows where row_num > 1
ALTER TABLE world_layoffso.layoffs_staging ADD row_num INT;

-- Create a new staging table to work with and assign row numbers
CREATE TABLE world_layoffso.layoffs_staging2 (
    company TEXT,
    location TEXT,
    industry TEXT,
    total_laid_off INT,
    percentage_laid_off TEXT,
    `date` TEXT,
    stage TEXT,
    country TEXT,
    funds_raised_millions INT,
    row_num INT
);

-- Insert data from the original staging table, adding row numbers
INSERT INTO world_layoffso.layoffs_staging2
(company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num)
SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
       ROW_NUMBER() OVER (
           PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
       ) AS row_num
FROM world_layoffso.layoffs_staging;

SET SQL_SAFE_UPDATES = 0;

-- Delete rows where row_num >= 2
DELETE FROM world_layoffso.layoffs_staging2
WHERE row_num >= 2;

-- Step 5: Standardize Data

-- Fix empty industry fields to NULL
UPDATE world_layoffso.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Fill NULL industry values by updating from another row with the same company
UPDATE world_layoffso.layoffs_staging2 t1
JOIN world_layoffso.layoffs_staging2 t2
    ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
    AND t2.industry IS NOT NULL;

-- Standardize variations of 'Crypto' in the industry column
UPDATE world_layoffso.layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- Standardize country names by trimming any trailing periods
UPDATE world_layoffso.layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- Step 6: Fix Date Format (String to Date)
UPDATE world_layoffso.layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Change the column data type from string to proper DATE
ALTER TABLE world_layoffso.layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Step 7: Check for Null Values
SELECT * 
FROM world_layoffso.layoffs_staging2
WHERE total_laid_off IS NULL
    AND percentage_laid_off IS NULL;

-- Delete rows with NULL values in both total_laid_off and percentage_laid_off
DELETE FROM world_layoffso.layoffs_staging2
WHERE total_laid_off IS NULL
    AND percentage_laid_off IS NULL;

-- Step 8: Drop the row_num column after it's no longer needed
ALTER TABLE world_layoffso.layoffs_staging2
DROP COLUMN row_num;

-- Final Check: View the cleaned data
SELECT * 
FROM world_layoffso.layoffs_staging2;
