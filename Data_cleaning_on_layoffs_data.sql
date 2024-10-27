use layoffs_database;

select *
from layoffs;

-- Data cleaning
-- 1. Remove duplicates
-- 2. Strandadize the data
-- 3. Handling null values
-- 4. Remove any column

-- Removing Dupiclates

create table layoffs_clone
like layoffs;

select *
from layoffs_clone;

insert into layoffs_clone
select * from layoffs;

select * from layoffs_clone;

with duplicate_cte as(
select *, 
row_number() over (partition by company, location, industry, total_laid_off,
date, stage, country, funds_raised_millions) as row_num
from layoffs_clone
)

select *
from duplicate_cte
where row_num > 1;

drop table layoffs_clone2;

CREATE TABLE `layoffs_clone2` (
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

select * from layoffs_clone2;

insert into layoffs_clone2
select *, 
row_number() over (partition by company, location, industry, total_laid_off,
date, stage, country, funds_raised_millions) as row_num
from layoffs_clone;

select * from layoffs_clone2
where row_num > 1;

delete 
from layoffs_clone2
where row_num > 1;

use layoffs_database;

select * from layoffs_clone2;

-- Standardizing data

select distinct company, Trim(company) 
from layoffs_clone2;

update layoffs_clone2
set company = Trim(company);

select * from layoffs_clone2;

select distinct location
from layoffs_clone2
where location like '%dorf' or location like 'mal%'
order by 1; 

update layoffs_clone2
set location = 'Dusseldorf'
where location like '%sseldorf';

update layoffs_clone2
set location = 'Malmo'
where location like 'Malm%';

update layoffs_clone2
set location = Trim(location);

select distinct location
from layoffs_clone2
order by 1;

select distinct industry 
from layoffs_clone2
order by 1;

select industry
from layoffs_clone2
where industry like 'Crypto%';

update layoffs_clone2
set industry = 'Crypto'
where industry like 'Crypto%';

select *
from layoffs_clone2;

select distinct date, str_to_date(date, '%m/%d/%Y')
from layoffs_Clone2
order by 1;

update layoffs_clone2
set date = str_to_date(date, '%m/%d/%Y');

Alter table layoffs_clone2
modify column date date;

select * from layoffs_clone2;

select distinct country
from layoffs_clone2
order by 1;

update layoffs_clone2
set country = 'United states'
where country like 'United states%';

select *
from layoffs_clone2;

select distinct stage
from layoffs_clone2
order by 1;

-- Handling missing values and NULL values

select *
from layoffs_clone2;

select distinct industry
from layoffs_clone2
order by 1;

select *
from layoffs_clone2
where industry is NULL or industry = '';

update layoffs_clone2
set industry = NUll
where industry = '';

select *
from layoffs_clone2
where company = 'Airbnb';

select *
from layoffs_clone2 t1
join layoffs_clone2 t2
on t1.company = t2.company
and t1.location = t2.location
where t1.industry is null 
and t2.industry is not null;

select t1.industry, t2.industry
from layoffs_clone2 t1
join layoffs_clone2 t2
on t1.company = t2.company
and t1.location = t2.location
where t1.industry is null 
and t2.industry is not null;

update layoffs_clone2 t1
join layoffs_clone2 t2
on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null 
and t2.industry is not null;

select *
from layoffs_clone2
where industry is null;

select *
from layoffs_clone2;

select *
from layoffs_clone2
where total_laid_off is null 
and percentage_laid_off is null;

delete
from layoffs_clone2
where total_laid_off is null 
and percentage_laid_off is null;

-- Removing column row_num

Alter table layoffs_clone2
drop column row_num;







