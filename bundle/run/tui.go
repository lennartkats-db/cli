package run

import (
	"github.com/pterm/pterm"
)

var highlightCounter int

func animateText(text string) string {
	colors := []pterm.Color{
		pterm.FgBlue,
		pterm.FgCyan,
		pterm.FgLightBlue,
		pterm.FgCyan,
	}
	var highlightedText string
	for i, char := range text {
		style := pterm.NewStyle(colors[(i+highlightCounter)%len(colors)])
		highlightedText += style.Sprint(string(char))
	}
	highlightCounter++
	return highlightedText
}