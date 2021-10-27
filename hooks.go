package spinix

type BeforeDetectFunc func(device *Device, rule *Rule)

type AfterDetectFunc func(device *Device, rule *Rule, match bool, events []Event)
