package regex

import "regexp"

type Pattern struct {
	ptn *regexp.Regexp
}

type Match struct {
	matches []string
}

func MustCompile(rule string) *Pattern {
	ptn := regexp.MustCompile(rule)
	return &Pattern{ptn: ptn}
}

func Compile(rule string) (*Pattern, error) {
	ptn, err := regexp.Compile(rule)
	if err != nil {
		return nil, err
	}
	return &Pattern{ptn: ptn}, nil
}

func (p *Pattern) Match(value string) *Match {
	matches := p.ptn.FindStringSubmatch(value)
	if len(matches) == 0 {
		return nil
	}
	return &Match{matches: matches}
}

func (m *Match) Group(n int) string {
	if len(m.matches) <= n {
		return ""
	}
	return m.matches[n]
}
