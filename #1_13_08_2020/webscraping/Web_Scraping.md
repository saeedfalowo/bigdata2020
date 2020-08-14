# E-commerce Web Scraping

The task was to use python to access the website "https://slickdeals.net"
and extract all the product deal names, price, shipping, and likes count
and then save within a csv file

## Method
The method used involved inspecting the product tiles element within the
webpage to find out the div property, class or id, common for all the deal
divs.
It was found that the section of the webpage where all the product tiles
are displayed is a single div with these parameters
'class' :'fpMainContent', 'id':'fpMainContent'

And all the product tile divs have these common parameter:
'class':'fpItem'

The python library beautiful soup was used to extract the required data
from the webpage source code

Using a for loop to iterate through all the divs with the same parameter,
the name, price, shipping, and likes count data were extracted by looking
into the html tags these data reside in along with their common class parameter

These extracted data were then collected in a string variable and written into a
txt and csv file

## Result
The webpage shows five sections where all the deals are displayed in tiles
- Feature Deals
- Just For You
- Frontpage Slickdeals
- Deal Sampler
- Deals You May Have Missed
However, only one of the sections can be seen in the source code.
- Frontpage Slickdeals

The data were extracted and stored into csv and text files, however there
were still some html entities in the string data that proved difficult to 
convert to unicode entities

