# georule
GPS/GEO based minimalist rules engine

# Status
Work in progress ...

# Table of contents
- [Variables](#variables)
    * [{device.speed}](#devicespeed)
    * [{device.status}](#devicestatus)
- [Functions](#functions)
    * [speed(min, max), speed(max)](#speedmin-max-speedmax)

## Variables
#### {device.speed}
filter by device speed (km/h)

Example:
```shell script
{device.speed} >= 0 AND {device.speed} <= 50
```

#### {device.status}
Filter by device status

Example:
```shell script
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

