package list

// Add appends Element to the list.
type Add struct {
	Element string
}

// Remove removes the first matching Element.
type Remove struct {
	Element string
}

// RemoveAt removes the element at Index. Invalid indexes leave state unchanged.
type RemoveAt struct {
	Index int
}

// Set replaces the element at Index. Invalid indexes leave state unchanged.
type Set struct {
	Index   int
	Element string
}

// Clear removes every element.
type Clear struct{}

// Get returns a copy of the complete list.
type Get struct{}

// GetAt returns the element at Index.
type GetAt struct {
	Index int
}
