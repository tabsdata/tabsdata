//
// Copyright 2026 Tabsdata Inc.
//

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"

	"github.com/robfig/cron/v3"
)

const (
	plausibleCount  = 2500
	structuredCount = 3000
	durationCount   = 1500
	randomCount     = 1000
	typoCount       = 50
	randomLength    = 24
)

var fixed = []string{
	"", " ", "* * * * *", "*/15 * * * *", "0 0 1 1 *", "0 9-17 * * mon-fri", "0 0 * JAN,Jul SUN",
	"? * * * *", "*-5 * * * *", "1, * * * *", ", * * * *", "5/10 * * * *", "0 0 31 2 *", "\t0  0 * * *\n",
	"+5 * * * *", "-5 * * * *", "0 0 * * 7", "0 0 * * 5#3", "L * * * *", "15W * * * *", "H * * * *",
	"0 0 * * * *", "0 0 * *", "@yearly", "@annually", "@monthly", "@weekly", "@daily", "@midnight", "@hourly",
	"@YEARLY", "@Daily", "@daily ", " @daily", "@reboot", "@every", "@every ", "@every 5", "@every 5d",
	"@every .s", "@every 0", "@every -5m", "@every 1.5h", "@every .5s", "@every 9223372036854775807ns",
	"@every 9223372036854775808ns", "@every -9223372036854775808ns",
	"@every 9223372036854775808ns9223372036854775808ns", "@every 2562047h", "@every 2562048h",
	"@every 2µs", "@every 2μs", "@every 1h30m", "@every +1h", "@every 1.h", "@every 1..5h",
	"TZ=UTC 0 0 * * *", "CRON_TZ=UTC 0 0 * * *", "TZ=0", "TZ=", "CRON_TZ=Bad/Zone 0 0 * * *",
	"0 0 * * FRİ", "0 0 * * fri̇", "99999999999999999999 * * * *", "0 0 * * sun-sat",
	"0 0 * * sat-sun", "*/0 * * * *", "0-59/60 * * * *", "0 0 1-31/32 * *", "*//2 * * * *",
	"5--5 * * * *", "0 0 * * *", "0 0 * * *　", "0 0 * * 0x5", "0 0 * * 05", "0 0 * * 0006",
	"0\u00850 * * * *", "@every 1e3s", "@every 0.0s", "@every 00", "@every -0", "@every +0", "@every 0s",
	"@every 1h1", "@every 1H", "* * * * * ", "@", "@ every 5m", "jan * * * *", "0 0 * jan-dec/2 *",
	"0 0 * * mon/2", "0 0 * * mon-/2", "0 0 * * -mon",
}

var tokens = []string{
	"*", "?", "0", "1", "5", "7", "12", "13", "23", "24", "31", "32", "59", "60", "6", "00", "+3", "-",
	"/", ",", "jan", "DEC", "sun", "SAT", "Fri", "L", "W", "#", "H", "x", "", "99999999999999999999", "1e2",
}

type bounds struct {
	min, max int
	names    []string
}

var fieldBounds = []bounds{
	{0, 59, nil},
	{0, 23, nil},
	{1, 31, nil},
	{1, 12, []string{"jan", "FEB", "Mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"}},
	{0, 6, []string{"sun", "MON", "Tue", "wed", "thu", "fri", "sat"}},
}

var separators = []string{" ", " ", " ", "  ", "\t", "\n", " ", " ", ""}

var units = []string{"ns", "us", "µs", "μs", "ms", "s", "m", "h", "d", "", "H", "S"}

var amounts = []string{
	"0", "1", "5", "1.5", ".5", "5.", "00", "9223372036854775807", "9223372036854775808", "2562047",
	"2562048", ".", "", "18446744073709551616", "0.000000000000000000001", "123456789012345678901234567890",
}

var descriptors = []string{"@yearly", "@annually", "@monthly", "@weekly", "@daily", "@midnight", "@hourly", "@every 5m"}

var alphabet = []rune("0123456789*?-/,@ \t.LWH#jJaAnNsSuUmMhHTZ=_Cµ İeEvVrRyYdDiIlLoO")

func validate(schedule string) (valid bool) {
	defer func() {
		if recover() != nil {
			valid = false
		}
	}()
	if len(schedule) == 0 {
		return false
	}
	if _, err := cron.ParseStandard(schedule); err != nil {
		return false
	}
	return !strings.Contains(schedule, "TZ")
}

func pick[T any](random *rand.Rand, values []T) T {
	return values[random.IntN(len(values))]
}

func atom(random *rand.Rand) string {
	k := random.Float64()
	if k < 0.15 {
		return pick(random, tokens)
	}
	value := pick(random, tokens)
	if k < 0.45 {
		value += "-" + pick(random, tokens)
	}
	if random.Float64() < 0.4 {
		value += "/" + pick(random, tokens)
	}
	return value
}

func field(random *rand.Rand) string {
	atoms := make([]string, pick(random, []int{1, 1, 1, 2, 3}))
	for i := range atoms {
		atoms[i] = atom(random)
	}
	return strings.Join(atoms, ",")
}

func structured(random *rand.Rand) string {
	fields := make([]string, pick(random, []int{3, 4, 5, 5, 5, 5, 5, 6, 7}))
	for i := range fields {
		fields[i] = field(random)
	}
	schedule := strings.Join(fields, pick(random, separators))
	if random.Float64() < 0.05 {
		schedule = " " + schedule
	}
	if random.Float64() < 0.05 {
		schedule += " "
	}
	return schedule
}

func value(random *rand.Rand, b bounds) string {
	if b.names != nil && random.Float64() < 0.3 {
		return pick(random, b.names)
	}
	if random.Float64() < 0.1 {
		return fmt.Sprint(pick(random, []int{b.min - 1, b.max + 1}))
	}
	return fmt.Sprint(b.min + random.IntN(b.max-b.min+1))
}

func plausibleAtom(random *rand.Rand, b bounds) string {
	var atom string
	switch k := random.Float64(); {
	case k < 0.2:
		atom = pick(random, []string{"*", "?"})
	case k < 0.6:
		atom = value(random, b)
	default:
		atom = value(random, b) + "-" + value(random, b)
	}
	if random.Float64() < 0.3 {
		atom += "/" + fmt.Sprint(random.IntN(b.max+2))
	}
	return atom
}

func plausible(random *rand.Rand) string {
	fields := make([]string, len(fieldBounds))
	for i, b := range fieldBounds {
		atoms := make([]string, pick(random, []int{1, 1, 1, 2, 3}))
		for j := range atoms {
			atoms[j] = plausibleAtom(random, b)
		}
		fields[i] = strings.Join(atoms, ",")
	}
	return strings.Join(fields, pick(random, separators[:len(separators)-1]))
}

func duration(random *rand.Rand) string {
	var builder strings.Builder
	for range pick(random, []int{1, 1, 2, 3}) {
		builder.WriteString(pick(random, amounts) + pick(random, units))
	}
	value := builder.String()
	if random.Float64() < 0.3 {
		value = pick(random, []string{"-", "+", ""}) + value
	}
	return "@every " + value
}

func noise(random *rand.Rand) string {
	runes := make([]rune, random.IntN(randomLength+1))
	for i := range runes {
		runes[i] = pick(random, alphabet)
	}
	return string(runes)
}

func typo(random *rand.Rand, descriptor string) string {
	runes := []rune(descriptor)
	at := random.IntN(len(runes) + 1)
	return string(runes[:at]) + string(pick(random, alphabet)) + string(runes[at:])
}

func corpus() []string {
	random := rand.New(rand.NewPCG(5071, 5071))
	schedules := append([]string{}, fixed...)
	for range plausibleCount {
		schedules = append(schedules, plausible(random))
	}
	for range structuredCount {
		schedules = append(schedules, structured(random))
	}
	for range durationCount {
		schedules = append(schedules, duration(random))
	}
	for range randomCount {
		schedules = append(schedules, noise(random))
	}
	for _, descriptor := range descriptors {
		for range typoCount {
			schedules = append(schedules, typo(random, descriptor))
		}
	}
	seen := map[string]bool{}
	unique := schedules[:0]
	for _, schedule := range schedules {
		if !seen[schedule] {
			seen[schedule] = true
			unique = append(unique, schedule)
		}
	}
	return unique
}

func main() {
	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()
	for _, schedule := range corpus() {
		line, err := json.Marshal([]any{schedule, validate(schedule)})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Fprintln(out, string(line))
	}
}
