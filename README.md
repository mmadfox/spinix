# georule
GPS/GEO based minimalist rules engine

# work in progress ...

## Functions
@ - variable identifier

| Name                                                                   | Example                                            | Description                             |
|------------------------------------------------------------------------|----------------------------------------------------|-----------------------------------------|
| speed(min, max) OR speed(max)                                          | speed(0, 30) OR speed(30.5)                        | Device speed range 0 - 30 km/h          |
| batteryCharge(min, max) OR batteryCharge(max)                          | batteryCharge(2.2, 40) OR batteryCharge(30)        | Device battery charge range 2.2 - 40%   |
| intersectsLine(@lineID) OR intersectsLine(@lineID1, @lineID2)          | intersectsLine(@one) OR intersectsLine(@one, @two) | Device crosses the line @one OR @two    |
| insidePolygon(@polygonID) OR insidePolygon(@polygonID1, @polygonID2)   | insidePolygon(@one) OR insidePolygon(@one, @two)   | Device inside the polygon @one OR @two  |
| outsidePolygon(@polygonID) OR outsidePolygon(@polygonID1, @polygonID2) | outsidePolygon(@one) OR outsidePolygon(@one, @two) | Device outside the polygon @one OR @two |
| emei(one, two, "three")                                                | emei(998f196f-b028-4641-9d16-36ee4344d3a1)         | Filter by device IMEI                   |
| owner(one, two, "three")                                               | owner(998f196f-b028-4641-9d16-36ee4344d3a1)        | Filter by device owner                  |
| brand(one, two, "three")                                               | brand(GPSv55Tracker, "GPS v56 Tracker")            | Filter by device brand                  |