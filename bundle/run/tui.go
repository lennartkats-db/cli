package run

import (
	"github.com/pterm/pterm"
)

var highlightCounter int

func animateText(text string) string {
	// This is a simple implementation of a highlight effect.
	// It cycles through a few shades of gray.
	colors := []pterm.Color{
		pterm.FgGray,
		pterm.FgLightWhite,
		pterm.FgWhite,
		pterm.FgLightWhite,
	}
	var highlightedText string
	for i, char := range text {
		style := pterm.NewStyle(colors[(i+highlightCounter)%len(colors)])
		highlightedText += style.Sprint(string(char))
	}
	highlightCounter++
	return highlightedText
}