package detector

import (
	"FLAC/configs"
	"sync"
)

type LevelStateManager struct {
	mu     sync.Mutex
	states map[string]*LevelStateMachine
}

func NewLSMManger(parts []string) *LevelStateManager {
	res := &LevelStateManager{
		mu:     sync.Mutex{},
		states: make(map[string]*LevelStateMachine),
	}
	for _, s := range parts {
		res.states[s] = NewLSM(res)
	}
	return res
}

// Start get the common levels from shardSet
func (c *LevelStateManager) Start(shardSet []string) (Level, int) {
	return c.synLevels(shardSet)
}

var TimeStamp4NFRec = 0

func (c *LevelStateManager) AsyNF(result *KvRes, ts int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ts != TimeStamp4NFRec {
		// avoid CF, nF from earlier stages.
		return
	}

	i := result.ID
	configs.CheckError(c.states[i].Next(true, true, c.states[i].level, i))
}

// Finish finish one round for the state machines
func (c *LevelStateManager) Finish(shardSet []string, results *KvResult, comLevel Level, ts int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ts != TimeStamp4NFRec {
		// avoid CF, nF from earlier stages.
		return nil
	}

	CrashF, NetF := make(map[string]bool), false // For level 3, no failure.

	if results != nil {
		CrashF, NetF = results.Analysis(shardSet, comLevel)
		for _, i := range shardSet {
			err := c.states[i].Next(CrashF[i], NetF, comLevel, i)
			if err != nil {
				return err
			}
		}
	} else {
		for _, i := range shardSet {
			err := c.states[i].Next(false, false, comLevel, i)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// synLevels get the common levels from shardSet
func (c *LevelStateManager) synLevels(shardSet []string) (Level, int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	configs.TPrintf("TryLock get for level sync")
	comLevel := NoCFNoNF
	for _, i := range shardSet {
		comLevel = MaxLevel(comLevel, c.states[i].GetLevel())
	}
	return comLevel, TimeStamp4NFRec
}
