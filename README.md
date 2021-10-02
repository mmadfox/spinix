# georule
GPS/GEO based minimalist rules engine

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
- [Functions](#functions)
    * [speed(min, max), speed(max)](#speedmin-max-speedmax)
    * [intersects(@id), intersectsLine(@id), intersectsPoint(@id), intersectsPoly(@id), intersectsRect(@id)](#intersectsid)
    * [within(@id), withinLine(@id), withinPoint(@id), withinPoly(@id), withinRect(@id)](#withinid)

## Operators
- ```AND```, ```OR```, ```NOT```, ```IN```
- ```==```, ```<```, ```>```,
- ```!=```, ```<=```, ```>=``` 

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


## Functions
@ - variable identifier

#### speed(min, max), speed(max)
*type: INT, FLOAT*

Filter by device speed (km/h)

Example:
```shell script
speed(0, 120) OR speed(300)
```

#### intersects(@id)

*type: @ID*

Detect intersection of device coordinates(lat, lon) with the geo object

- intersects(@id)
- intersectsLine(@id)
- intersectsPoint(@id)
- intersectsPoly(@id)
- intersectsRect(@id)

Example:
```gotemplate
intersectsPoly(@someid) OR intersectsLine(@someid) 
NOT intersectsPoly(@someid) OR NOT intersectsLine(@someid) 
```

#### within(@id)

*type: @ID*

Detect device coordinates(lat, lon) within the geo object

- within(@id)
- withinLine(@id)
- withinPoint(@id)
- withinPoly(@id)
- withinRect(@id)

Example:
```gotemplate
within(@id) OR withinPoly(@id)
NOT within(@id) AND NOT withinLine(@id) 
```