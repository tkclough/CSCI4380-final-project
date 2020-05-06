# Installation
To install dependencies, run `pip install -r requirements.txt`

# Loading data
After datasets have been downloaded to the `datasets` directory, load the data by running
`python load_data.py`.

# Using the application
Start the application by running `python application.py`. There are a number of available commands:
* precipitationVsNumIncidents: print a table of precipitation and incidents
* totalPrecipitationByState: print a table listing precipitation in each state
* propertyDamageVsPrecipitation: print a table of property loss and precipitation
* precipitationByCity '[City], [State]': print out the precipitation in a given city (in mm)
* totalIncidentsByCity: print how many incidents there have been in a city
