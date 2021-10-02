# georule
GPS/GEO based minimalist rules engine

## Status
Work in progress ...

## Table of contents
- [Operators](#operators)
- [Variables](#variables)
    * [{device.speed}](#devicespeed)
    * [{device.status}](#devicestatus)
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
Filter by device speed (km/h)

Example:
```gotemplate
{device.speed} >= 0 AND {device.speed} <= 50
```

#### {device.status}
Filter by device status

Example:
```gotemplate
{device.status} == 1 OR {device.status} IN [2,4]
{device.status} == 1 OR {device.status} == 2 OR {device.status} == 4
{device.status} == 1 AND {device.status} == 2 
{device.status} >= 0 AND {device.status} < 10
({device.status} == 1 OR {device.status} IN [2,4]) OR ({device.status} >= 0 AND {device.status} < 10)
```

## Functions
@ - variable identifier

#### speed(min, max), speed(max)
Filter by device speed (km/h)

Example:
```shell script
speed(0, 120) OR speed(300)
```

#### intersects(@id)

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