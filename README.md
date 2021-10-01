# georule
GPS/GEO based minimalist rules engine

# work in progress ...

## Functions
@ - variable identifier

| Name                                                                   | Example                                            |
|------------------------------------------------------------------------|----------------------------------------------------|
| speed(min, max) OR speed(max)                                          | speed(0, 30) OR speed(30.5)                        |
| batteryCharge(min, max) OR batteryCharge(max)                          | batteryCharge(2.2, 40) OR batteryCharge(30)        |
| intersectsLine(@lineID) OR intersectsLine(@lineID1, @lineID2)          | intersectsLine(@one) OR intersectsLine(@one, @two) |
| insidePolygon(@polygonID) OR insidePolygon(@polygonID1, @polygonID2)   | insidePolygon(@one) OR insidePolygon(@one, @two)   |
| outsidePolygon(@polygonID) OR outsidePolygon(@polygonID1, @polygonID2) | outsidePolygon(@one) OR outsidePolygon(@one, @two) |