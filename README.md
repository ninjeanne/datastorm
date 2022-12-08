# DataStorm

## Problem Statement
Canada has always been a safe haven for refugees during war and crisis. In light of recent events, more than 32000 Ukrainians have found support and a safe place to live here. “In 2019, Canada once again came out as the world leader in the resettlement of refugees, ranking first among 26 countries. Last year, Canada provided 30,082 refugees with the opportunity to build a new life for themselves and their families, including through its private sponsorship program, which accounted for 58 percent over the past ten years.“ - UNHCR. While Canada is a large country, most of it is not very suitable for living, especially for those seeking refuge. In addition, with climate change on the rise, the regions once considered habitable by default might need to be re-evaluated and regularly updated. This project aims to provide a data-backend study of different locations in Canada where the government could expand cities and build suitable shelters. The factors to be considered would be the region’s weather, including temperature, rainfall, snowfall, and snow depth and the occurrence of disasters like cyclones, tornadoes, and blizzards. With this information, the government of Canada could spend resources efficiently on setting up transportation, crops and livestock, infrastructure and housing, and sustainable energy sources.

For this project, we will be using the GHCN (Global Historical Climatology Network) -daily dataset compiled for 180 countries and more than 100,000 stations by NCEI. The dataset has numerous variables some of which are- maximum/minimum, the temperature at observation time, precipitation, snowfall, snow depth, evaporation, wind movement, wind maximums, soil temperature, cloudiness, and more. The records of about 175 years have been compiled under this dataset. 

A descriptive analysis of this nature might need to be performed regularly due to the changing climatic conditions and demographic of Canada. The GHCNd dataset is a comprehensive set of observations recorded over several locations, but to remove meaningful conclusions to solve our problem statement, we may need a more scalable alternative. With the help of the big data tools we have dabbled in during this course CMPT 732- Programming for Big Data 1 and more, we have designed a pipeline adhering to the 4-Vs volume, velocity, variety and veracity. 

This proposed methodology could be further modified to better understand any geographical region’s weather conditions. From this dataset we have selected three elements- Wind, Snow and Precipitation and analyzed them for this project.

1. [Project Proposal](proposal)
2. [Running and Testing](RUNNING.md)
3. [Data source](Acquiring)
4. [Extract-Transform-Load (ETL)](ETL)
5. [Technologies](Technologies)
6. [Bigness and Parallelization](Parallelization)
7. [UI and Visualization](Visualization)
