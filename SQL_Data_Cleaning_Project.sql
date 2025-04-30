--- Cleaning Data in SQL Queries ---
-- 1. Remove Duplicates
-- 2. Standardize The Data
-- 3. Remove NULL Values
-- 4. Remove Unused Columns


SELECT* FROM layoffs;
CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT INTO layoffs_staging
SELECT* FROM layoffs; 

SELECT * FROM layoffs_staging;

-------------- Remove the Duplicates ----------------

SELECT * ,                 ---Used Window() function to partition by each column and row_number() to assign number to identify duplicates.
ROW_NUMBER() 
OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging;

WITH duplicate_CTE AS      ---Used a CTE to apply filter on the row_num column and enhance readability
(
SELECT * ,
ROW_NUMBER() 
OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT*
FROM duplicate_CTE
WHERE row_num>1;


CREATE TABLE `layoffs_staging2` (    ---- Created another table with row_num column 
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2
SELECT * ,
ROW_NUMBER() 
OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging;


SELECT*
FROM layoffs_staging2
WHERE row_num>1;


delete                 ----- Deleted the duplicated rows
FROM layoffs_staging2
WHERE row_num>1;


------------- Standardizing The Data --------------

SELECT company,TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);


SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';


UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;


SELECT DISTINCT country ,TRIM(TRAILING '.'FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.'FROM country)
WHERE country LIKE 'United States%';


SELECT *
FROM layoffs_staging2;

SELECT `date`
FROM layoffs_staging2
ORDER BY 1;


UPDATE layoffs_staging2       ----- Updated Date column from text to date format using STR_TO_DATE()
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2
ORDER BY 1;


ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


--------------- Remove NULL values ------------------

SELECT* FROM layoffs_staging2
WHERE total_laid_off  IS NULL
AND percentage_laid_off  IS NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';


SELECT*
FROM layoffs_staging2
WHERE company LIKE 'Bally%';


SELECT t1.industry,t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
    ON t1.company = t2.company
WHERE ( t1.industry IS NULL OR t2.industry = '')
AND t2.industry IS NOT NULL;    


UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';


UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
    ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;     


SELECT* FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


DELETE
FROM layoffs_staging2
WHERE total_laid_off IS  NULL
AND percentage_laid_off IS NULL;

SELECT*
FROM layoffs_staging2;

---------- Delete Unused columns ------------

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


SELECT*                ----- Cleaned Dataset -----
FROM layoffs_staging2;


-------------------------------------------------------------------------------------------
--------------------------------- Exploratory Data Analysis -------------------------------

SELECT*
FROM layoffs_staging2;


SELECT MAX(total_laid_off),MAX(percentage_laid_off)
FROM layoffs_staging2;

SELECT*
FROM layoffs_staging2
WHERE percentage_laid_off=1
ORDER BY funds_raised_millions DESC;

SELECT company,sum(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT MIN(`date`),MAX(`date`)
FROM layoffs_staging2;

SELECT industry,SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;


SELECT country,sum(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT*
FROM layoffs_staging2;

SELECT YEAR (`date`),sum(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;


SELECT stage,SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY  2 desc;


SELECT company,AVG(percentage_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;


SELECT SUBSTRING(`date`,1,7) AS `MONTH`,sum(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;


WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`,1,7) AS `MONTH`,SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`,total_off,
SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total;


SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;


















