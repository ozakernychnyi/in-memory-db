package in_memory

// using default GO map as in_memory db.
// It fits well for current purpose.
type storage map[string]string

// transaction - struct that used to keep certain transaction state.
// It has parent and nested fields to emulate transactions (including nested) chain.
type transaction struct {
	parent *transaction
	nested *transaction
	// elements which going to be added in scope of current transaction
	toSet map[string]string
	// elements which going to be deleted in scope of current transaction
	toDelete map[string]struct{}
}

// InMemory - in-memory db struct
type InMemory struct {
	storage            storage
	onGoingTransaction *transaction
}

// InMemoryDatabase - creates InMemory db instance
func InMemoryDatabase() *InMemory {
	return &InMemory{
		storage: make(storage),
	}
}

// Get - returns a value from db by provided key.
// Method returns actual value considering on which level of transaction it was executed.
func (db *InMemory) Get(key string) string {
	// if there is no running transaction just return value right from storage
	if db.onGoingTransaction == nil {
		return db.storage[key]
	}

	// get the last element from transactions chain to retrieve the most actual data
	tr := db.getLast()

	// if element exists in toSet map that means it was added in current or nested transaction. Just return it
	if v, ok := tr.toSet[key]; ok {
		return v
	}

	// if element exists in toDelete map that means it was added in current or nested transaction. We shouldn't return
	// this value as we consider it as deleted on current execution point.
	if _, ok := tr.toDelete[key]; ok {
		return ""
	}

	// if a value was not found in toSet or toDelete maps that means that there were no actions with it in downstream transactions
	// and just return what exists in storage now.
	return db.storage[key]
}

// Set - sets a value by provided key.
// If set was executed during certain transaction this key-value pair will be placed in toSet temporary collection of this transaction.
func (db *InMemory) Set(key, value string) {
	if db.onGoingTransaction == nil {
		db.storage[key] = value
		return
	}

	tr := db.getLast()
	tr.toSet[key] = value
	// need to delete possibly present key-value pair from toDelete collection to be consistent.
	delete(tr.toDelete, key)
}

// Delete - deletes a value by provided key
// If delete was executed during certain transaction this key-value pair will be placed in toDelete temporary collection of this transaction.
func (db *InMemory) Delete(key string) {
	if db.onGoingTransaction == nil {
		delete(db.storage, key)
		return
	}

	tr := db.getLast()
	tr.toDelete[key] = struct{}{}
	// need to delete possibly present key-value pair from toSet collection to be consistent.
	delete(tr.toSet, key)
}

// StartTransaction - starts new transaction.
func (db *InMemory) StartTransaction() {
	tr := db.getLast()
	// if tr == nil it means there is no running transaction at this execution point
	if tr == nil {
		db.onGoingTransaction = &transaction{
			toSet:    make(storage),
			toDelete: make(map[string]struct{}),
		}
		return
	}

	// add a nested transaction to the transactions chain
	tr.nested = &transaction{
		parent:   tr,
		toDelete: copyToDeleteMap(tr.toDelete),
		toSet:    copyToSetMap(tr.toSet),
	}
}

// Commit - commits transaction.
func (db *InMemory) Commit() {
	tr := db.getLast()
	if tr == nil {
		return
	}

	// if the last transaction node doesn't have pointer to parent it means we are committing the very first transaction
	if tr.parent == nil {
		// apply deleting elements from storage
		for k := range tr.toDelete {
			delete(db.storage, k)
		}

		// apply setting elements to storage
		for k, v := range tr.toSet {
			db.storage[k] = v
		}

		// as we committed head transaction set onGoingTransaction to nil
		db.onGoingTransaction = nil

		return
	}

	// transfer toDelete data from current transaction to parent
	for k := range tr.toDelete {
		tr.parent.toDelete[k] = struct{}{}
		// if in parent transaction already exists a key we wanted to set within it but in nested transaction this key
		// intended to be deleted we should delete it in parent to have consistent state across transactions.
		delete(tr.parent.toSet, k)
	}

	// transfer toSet data from current transaction to parent
	for k, v := range tr.toSet {
		tr.parent.toSet[k] = v
		// if in parent transaction already exists a key we wanted to delete within it but in nested transaction this key
		// intended to be set we should delete it in parent to have consistent state across transactions.
		delete(tr.parent.toDelete, k)
	}

	// nullify current transaction in parent as we are done with it
	tr.parent.nested = nil
}

// Rollback - rollbacks transaction
func (db *InMemory) Rollback() {
	tr := db.getLast()
	if tr == nil {
		return
	}

	// if the last transaction node doesn't have pointer to parent it means we are performing rollback of the very first transaction
	if tr.parent == nil {
		// set onGoingTransaction to nil to mark whether there is no alive transaction
		db.onGoingTransaction = nil
		return
	}

	// nullify nested transaction in parent
	tr.parent.nested = nil
}

// returns the last transaction node from chain
func (db *InMemory) getLast() *transaction {
	if db.onGoingTransaction == nil {
		return nil
	}

	last := db.onGoingTransaction

	for last != nil {
		if last.nested == nil {
			return last
		}
		last = last.nested
	}

	return nil
}

func copyToSetMap(toSet map[string]string) map[string]string {
	copied := make(map[string]string)

	for k, v := range toSet {
		copied[k] = v
	}

	return copied
}

func copyToDeleteMap(toDelete map[string]struct{}) map[string]struct{} {
	copied := make(map[string]struct{})

	for k := range toDelete {
		copied[k] = struct{}{}
	}

	return copied
}
