# Spinix

GPS/GEO minimalist rules engine

## Status
Work in progress ...

## Table of contents
- [Operators](#operators)
- [Variables](#variables)
    * [{device.speed}](#devicespeed)
    * [{device.status}](#devicestatus)
    * [{device.owner}](#deviceowner)
    * [{device.brand}](#devicebrand)
    * [{device.model}](#devicemodel)
    * [{device.emei}](#deviceemei)
    * [{device.battery}](#devicebattery)
    * [{device.temperature}](#devicetemperature)
    * [{device.humidity}](#devicehumidity)
    * [{device.luminosity}](#deviceluminosity)
    * [{device.pressure}](#devicepressure)
    * [{device.fuellevel}](#devicefuellevel)
- [Functions](#functions)
   
## Operators
- ```AND```, ```OR```, ```NOT```, ```IN```
- ```==```, ```<```, ```>```,
- ```!=```, ```<=```, ```>=```, 
- ```INSIDE```, ```OUTSIDE```, ```INTERSECTS```
- ```+```, ```-```, ```*```, ```/\```, ```%```

## Variables
#### {device.speed} 
*type: INT, FLOAT*

Filter by device speed (km/h)

Example:
```gotemplate
{device.speed} >= 0 AND {device.speed} <= 50
```

#### {device.status}
*type: INT*

Filter by device status

Example:
```gotemplate
{device.status} == 1 OR {device.status} IN [2,4]
{device.status} == 1 OR {device.status} == 2 OR {device.status} == 4
{device.status} == 1 AND {device.status} == 2 
{device.status} >= 0 AND {device.status} < 10
({device.status} == 1 OR {device.status} IN [2,4]) OR ({device.status} >= 0 AND {device.status} < 10)
```

#### {device.owner}
*type: STRING*

Filter by device owner

Example:
```gotemplate
{device.owner} == "5597dfe5-ef3b-41a4-9a31-2d926d8edd74"
{device.owner} IN ["5597dfe5-ef3b-41a4-9a31-2d926d8edd74", "5597dfe5-ef3b-41a4-9a31-2d926d8edd74", "5597dfe5-ef3b-41a4-9a31-2d926d8edd74"]
{device.owner} NOT IN ["5597dfe5-ef3b-41a4-9a31-2d926d8edd74", "5597dfe5-ef3b-41a4-9a31-2d926d8edd74", "5597dfe5-ef3b-41a4-9a31-2d926d8edd74"]
({device.owner} == "5597dfe5-ef3b-41a4-9a31-2d926d8edd74" AND {device.owner} == "5597dfe5-ef3b-41a4-9a31-2d926d8edd74") OR ({device.owner} == "5597dfe5-ef3b-41a4-9a31-2d926d8edd74")
```

#### {device.brand}
*type: STRING*

Filter by device brand

Example:
```gotemplate
{device.brand} == "TrackerTOk"
{device.brand} IN ["TrackerTOk", "TrackerTOk2"]
{device.brand} NOT IN ["TrackerTOk", "TrackerTOk2"]
({device.brand} == "TrackerTOk" AND {device.brand} == "TrackerTOk2") OR ({device.brand} == "TrackerTOk4")
```

#### {device.model}
*type: STRING*

Filter by device model

Example:
```gotemplate
{device.model} == "Model v45-x1"
{device.model} IN ["Model v45-x1", "Model v45-x2"]
{device.model} NOT IN ["Model v45-x1", "Model v45-x2"]
```

#### {device.emei}
*type: STRING*

Filter by device EMEI

Example:
```gotemplate
{device.emei} == "AA-BBBBBB-CCCCCC-D"
{device.emei} IN ["AA-BBBBBB-CCCCCC-D"]
{device.emei} NOT IN ["AA-BBBBBB-CCCCCC-D"]
```

#### {device.battery}
*type: FLOAT*

Filter by device battery charge

Example:
```gotemplate
{device.battery} >= 0 AND {device.battery} <= 50  
```

#### {device.temperature}
*type: FLOAT*

Filter by device temperature

Example:
```gotemplate
{device.temperature} >= 0 AND {device.temperature} <= 50  
```

#### {device.humidity}
*type: FLOAT*

Filter by device humidity

Example:
```gotemplate
{device.humidity} >= 0 AND {device.humidity} <= 50  
```

#### {device.luminosity}
*type: FLOAT*

Filter by device luminosity

Example:
```gotemplate
{device.luminosity} >= 0 AND {device.luminosity} <= 50  
```

#### {device.pressure}
*type: FLOAT*

Filter by device pressure

Example:
```gotemplate
{device.pressure} >= 0 AND {device.pressure} <= 50  
```

#### {device.fuellevel}
*type: FLOAT*

Filter by device fuellevel

Example:
```gotemplate
{device.fuellevel} >= 0 AND {device.fuellevel} <= 50  
```


## Functions
@ - variable identifier
