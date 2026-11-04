package loom

// ScheduleOptionsAt is scheduleOptions at a given now, for tests.
var ScheduleOptionsAt = scheduleOptions

// ToldOf is how many things w remembers telling.
func ToldOf(w *Watcher) int { return len(w.told) }
