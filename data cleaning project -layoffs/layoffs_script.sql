-- DATA Cleaning
use layoffs;
select * from layoffs;

-- Remove Duplicates

create table layoffs_stagging
like layoffs;

select * from layoffs_stagging;

insert  layoffs_stagging
select * from layoffs;

select *,
row_number() over(partition by company,
location,industry,total_laid_off,percentage_laid_off,`date`,stage,country)
from layoffs_stagging;


with duplicates_cte as
(
select *,
row_number() over(partition by company,
location,industry,total_laid_off,percentage_laid_off,`date`,stage,country) as row_num
from layoffs_stagging
)
select * from duplicates_cte
where row_num >1;

select * from layoffs_stagging
where company ='hibob';

CREATE TABLE `layoffs_stagging2` (
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

insert layoffs_stagging2
select *,
row_number() over(partition by company,
location,industry,total_laid_off,percentage_laid_off,`date`,stage,country) as row_num
from layoffs_stagging;

select * from layoffs_stagging2
where row_num>1;

delete from layoffs_stagging2
where row_num >1;

select * from layoffs_stagging2
where company ='hibob';

select * from layoffs_stagging2;

-- Standardize data

select distinct(company), trim(company) from layoffs_stagging2;

update layoffs_stagging2 
set company=trim(company);
select * from layoffs_stagging2;

select distinct(industry) from layoffs_stagging2 
order  by 1;

select distinct(industry) from layoffs_stagging2  where industry 
like "fin%";
update layoffs_stagging2 
set industry= "Crypto"
where  industry like "crypto%";

update layoffs_stagging2 
set industry="Finance"
where industry like "fin%";

select distinct(country) from layoffs_stagging2 
order  by 1;

select distinct(country),trim(trailing '.' from country)
from layoffs_stagging2
order by 1 ;
update layoffs_stagging2 
set country = trim(trailing '.' from country);

select distinct(stage) from layoffs_stagging2 
order  by 1;

-- date --

select `date`, str_to_date(`date`,'%m/%d/%Y')
from layoffs_stagging2;

update layoffs_stagging2
set `date`= str_to_date(`date`, '%m/%d/%Y');
select * from layoffs_stagging2;

-- now change the dt of date to date -- -- not in raw table --

alter table layoffs_stagging2
modify column `date` DATE;

-- handling null or no values --

select * from layoffs_stagging2 where industry is NULL or industry ='';

select * from layoffs_stagging2
where industry is null or
industry = '';

update layoffs_stagging2
set industry= null
where industry = '';


select * from layoffs_stagging2
where company like 'ball%';

select * from layoffs_stagging2 t1
join layoffs_stagging2 t2
on t1.company = t2.company and
   t1.location = t2.location
where t1.industry is null and
t2.industry is not null;

update layoffs_stagging2 t1
join layoffs_stagging2 t2
on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null and
  t2.industry is not null;

-- deleting rows --

select * from layoffs_stagging2
where total_laid_off is null and
percentage_laid_off is null;  
-- we dont know if the company did layoff or not so we can del those --

delete from layoffs_stagging2
where total_laid_off is null and
percentage_laid_off is null;

select * from  layoffs_stagging2;

alter table layoffs_stagging2
drop column row_num;











