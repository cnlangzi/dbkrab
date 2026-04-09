package sql

import "sync"

// Skills aggregates skill operations with internal locking.
type Skills struct {
	mu    sync.RWMutex
	items map[string]*Skill // key = skill.Id
}

// NewSkills creates a new empty Skills collection.
func NewSkills() *Skills {
	return &Skills{
		items: make(map[string]*Skill),
	}
}

// Get returns a skill by its Id. Returns (nil, false) if not found.
func (s *Skills) Get(id string) (*Skill, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	skill, ok := s.items[id]
	return skill, ok
}

// List returns all skills. The returned slice is a copy for thread-safe iteration.
func (s *Skills) List() []*Skill {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*Skill, 0, len(s.items))
	for _, skill := range s.items {
		result = append(result, skill)
	}
	return result
}

// Set adds or updates a skill in the collection.
func (s *Skills) Set(skill *Skill) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[skill.Id] = skill
}

// Delete removes a skill by its Id.
func (s *Skills) Delete(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.items, id)
}

// Len returns the number of skills in the collection.
func (s *Skills) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.items)
}
