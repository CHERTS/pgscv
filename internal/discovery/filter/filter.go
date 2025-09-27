// Package filter implements regexp matching cluster and database name
package filter

import "regexp"

// Filter struct implementing regexp filters for clusters and theirs databases
type Filter struct {
	nameRegexp        *regexp.Regexp
	dbRegexp          *regexp.Regexp
	excludeNameRegexp *regexp.Regexp
	excludeDbRegexp   *regexp.Regexp
}

// New return Filter structure with compiled regexps
func New(name string, db, excludeName, excludeDb *string) *Filter {
	f := &Filter{}
	f.nameRegexp = regexp.MustCompile(name)
	if db != nil {
		f.dbRegexp = regexp.MustCompile(*db)
	}
	if excludeName != nil {
		f.excludeNameRegexp = regexp.MustCompile(*excludeName)
	}
	if excludeDb != nil {
		f.excludeDbRegexp = regexp.MustCompile(*excludeDb)
	}
	return f
}

// MatchName check name is matched name regexp and not matched exclude name regexp
func (f *Filter) MatchName(name string) bool {
	if f.excludeNameRegexp != nil && f.excludeNameRegexp.MatchString(name) {
		return false
	}
	return f.nameRegexp.MatchString(name)
}

// MatchDb check database is matched name regexp and not matched exclude name regexp
func (f *Filter) MatchDb(name string) bool {
	if f.excludeDbRegexp != nil && f.excludeDbRegexp.MatchString(name) {
		return false
	}
	if f.dbRegexp == nil {
		return true
	}
	return f.dbRegexp.MatchString(name)
}
