--Analyze and categorize the data by continent for a more detailed understanding.
select location, max(total_deaths)  as TotalDeathCount
 from portafolio.dbo.CovidDeaths  where continent is   null
 group by location 
order by 2 desc

--Identify the locations with the highest infection rates.
select location,Population, max(total_cases) ,max((total_cases/population)*100) as InfectionRate
 from portafolio.dbo.CovidDeaths 
 group by location,population 
order by 4 desc


--Showing ranking countries based on  the number of deaths at the end of 2020  
--DENSE_RANK to avoid skip any number
SELECT location, max(total_deaths) 'Total Deaths',
     dense_rank() OVER (
                    ORDER BY max(total_deaths) desc) 
              AS Ranking
  FROM  portafolio.dbo.CovidDeaths
 WHERE date = '2020-12-31' and continent is not null
 group by location


--Showing the Total deaths by Location based on the iso_code O
--This code defined regions 
select location,max(total_deaths) as TotalDeaths from portafolio.dbo.CovidDeaths  
where  LEFT(iso_code,5)='OWID_'
group by [location]


--Examine the relationship between the total number of cases and the population size.
--Illustrate the percentage of the population that has been affected by COVID-19.
select location,date, total_cases,Population, (total_cases/population)*100 as InfectionRate
 from portafolio.dbo.CovidDeaths 
 where location='Ecuador'
 order by 1,2


--Showing the evolution of number of new_cases by month in Ecuador
select CONVERT(VARCHAR(7),date,126) as Month, sum(new_Cases) as NumberOfCases--,total_cases-- MAX(new_cases)  
from portafolio.dbo.CovidDeaths 
 where location='Ecuador'  
 GROUP BY CONVERT(VARCHAR(7),date,126)


-- Compute the percentage of deaths in Ecuador relative to the total number of cases.
select date, total_cases,total_deaths, 
cast(round ((total_deaths/total_cases)*100,2)as numeric(15,2)) as DeathPercentage
 from portafolio.dbo.CovidDeaths 
 where location ='Ecuador'
 order by 1,2


--Showing Countries with the Highest Death Count per Population
select Location, max(total_deaths)  as TotalDeathCount
 from portafolio.dbo.CovidDeaths  where continent is not null
 group by location 
order by 2 desc

--Present the countries that have the highest count of vaccinated people in proportion to their population.
select Location, max(people_fully_vaccinated)  as PeopleVaccinated
 from portafolio.dbo.CovidDeaths  where continent is not null
 group by location 
order by 2 desc

--Display the continents that have the highest COVID-19 case count relative to their population.
select continent, max(total_deaths) as TotalDeathCount
from portafolio.dbo.CovidDeaths  where continent is not null
 group by continent 
 order by TotalDeathCount desc 


--Calculate the quartile for total_deaths at the conclusion of the year 2020.
select location,total_deaths, ntile(4) over ( order by total_deaths desc ) as Quartile
from portafolio.dbo.CovidDeaths cv 
where date='2020-12-31' and continent is not null 
order by 2

--CTE
--Create a CTE to retrieve the highest count of cases per month. 
--Then, employ the LAG function to observe the variance from the preceding month.
with DiffCasesGlob (Location,MonthAnalyzed,CasesByMonth)
as (
select Location, CONVERT(VARCHAR(7),date,126) as MonthAnalyzed,
max(total_cases) as CasesByMonth 
FROM portafolio.dbo.CovidDeaths cv 
where continent is not null 
group by location,CONVERT(VARCHAR(7),date,126))

--use CTE
 select Location,MonthAnalyzed, CasesByMonth, 
 convert(int,CasesByMonth) -LAG(convert(int,CasesByMonth), 1)  over (partition by location order by MonthAnalyzed) as DiffLastMonth
 from DiffCasesGlob
 order by 1,2
 
---------

---temp table 
--Store the latest vaccination records for Africa in a temporary table with a rolling update

drop table if exists #RollingPopulationVaccinated
create table #RollingPopulationVaccinated
(
    Continent nvarchar(255),
    Location NVARCHAR(255),
    Date datetime,
    Population numeric,
    new_vaccinations numeric,
    RolRollingPeopleVaccinated numeric
)

insert into #RollingPopulationVaccinated
select cd.continent,cd.location,cd.date,cd.population ,cv.new_vaccinations,
sum(convert (int,cv.new_vaccinations)) over (PARTITION by cd.LOCATION order by cd.location, cd.date ) 
 as RollingPeopleVaccinated
from portafolio.dbo.CovidVaccinations cv
join portafolio.dbo.CovidDeaths cd 
on cv.location =cd.[location]
 and cd.date=cv.date
 where cd.continent= 'Africa' and cv.new_vaccinations is not null
 order by cd.location 


--Creating View to store data 
--Store the latest vaccination records for Africa in a temporary table with a rolling update
Create view RollingPopulationVaccinated AS
select cd.continent,cd.location,cd.date,cd.population ,cv.new_vaccinations,
sum(convert (int,cv.new_vaccinations)) over (PARTITION by cd.LOCATION order by cd.location, cd.date ) 
 as RollingPeopleVaccinated
from portafolio.dbo.CovidVaccinations cv
join portafolio.dbo.CovidDeaths cd 
on cv.location =cd.[location]
 and cd.date=cv.date
 where cd.continent= 'Africa' and cv.new_vaccinations is not null
 
