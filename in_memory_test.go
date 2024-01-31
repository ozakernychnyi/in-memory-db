package in_memory_test

import (
	"testing"

	in_memory "github.com/ozakernychnyi/in-memory-db"
	"github.com/stretchr/testify/require"
)

func TestCommittedTransaction(t *testing.T) {
	db := in_memory.InMemoryDatabase()

	db.Set("key1", "val1")
	require.Equal(t, "val1", db.Get("key1"))

	db.StartTransaction()
	db.Set("key1", "val2")
	require.Equal(t, "val2", db.Get("key1"))
	db.Commit()

	require.Equal(t, "val2", db.Get("key1"))
}

func TestRollbackTransaction(t *testing.T) {
	db := in_memory.InMemoryDatabase()

	db.Set("key1", "val1")
	require.Equal(t, "val1", db.Get("key1"))

	db.StartTransaction()
	db.Set("key1", "val2")
	require.Equal(t, "val2", db.Get("key1"))
	db.Rollback()

	require.Equal(t, "val1", db.Get("key1"))
}

func TestCommittedNestedTransaction(t *testing.T) {
	db := in_memory.InMemoryDatabase()

	db.Set("key1", "val1")
	require.Equal(t, "val1", db.Get("key1"))

	db.StartTransaction()
	db.Set("key1", "val2")
	require.Equal(t, "val2", db.Get("key1"))

	db.StartTransaction()
	require.Equal(t, "val2", db.Get("key1"))
	db.Delete("key1")
	require.Equal(t, "", db.Get("key1"))
	db.Commit()
	require.Equal(t, "", db.Get("key1"))

	db.Commit()
	require.Equal(t, "", db.Get("key1"))
}

func TestRollbackNestedTransaction(t *testing.T) {
	db := in_memory.InMemoryDatabase()

	db.Set("key1", "val1")
	require.Equal(t, "val1", db.Get("key1"))

	db.StartTransaction()
	db.Set("key1", "val2")
	require.Equal(t, "val2", db.Get("key1"))

	db.StartTransaction()
	require.Equal(t, "val2", db.Get("key1"))
	db.Delete("key1")
	require.Equal(t, "", db.Get("key1"))
	db.Rollback()
	require.Equal(t, "val2", db.Get("key1"))

	db.Commit()
	require.Equal(t, "val2", db.Get("key1"))
}

func TestCommittedDeeplyNestedTransaction(t *testing.T) {
	db := in_memory.InMemoryDatabase()

	db.Set("key1", "val1")
	require.Equal(t, "val1", db.Get("key1"))

	db.StartTransaction()
	db.Set("key1", "val2")
	db.Set("key3", "val3")
	require.Equal(t, "val2", db.Get("key1"))
	require.Equal(t, "val3", db.Get("key3"))

	db.StartTransaction()
	require.Equal(t, "val2", db.Get("key1"))
	db.Delete("key1")
	db.Set("key3", "val4")
	require.Equal(t, "", db.Get("key1"))
	require.Equal(t, "val4", db.Get("key3"))

	db.StartTransaction()
	db.Set("key1", "val5")
	db.Delete("key3")
	require.Equal(t, "val5", db.Get("key1"))
	require.Equal(t, "", db.Get("key3"))
	db.Commit()

	require.Equal(t, "val5", db.Get("key1"))
	require.Equal(t, "", db.Get("key3"))

	db.Commit()
	require.Equal(t, "val5", db.Get("key1"))
	require.Equal(t, "", db.Get("key3"))

	db.Commit()
	require.Equal(t, "val5", db.Get("key1"))
	require.Equal(t, "", db.Get("key3"))
}

func TestRollbackDeeplyNestedTransaction(t *testing.T) {
	db := in_memory.InMemoryDatabase()

	db.Set("key1", "val1")
	require.Equal(t, "val1", db.Get("key1"))

	db.StartTransaction()
	db.Set("key1", "val2")
	db.Set("key3", "val3")
	require.Equal(t, "val2", db.Get("key1"))
	require.Equal(t, "val3", db.Get("key3"))

	db.StartTransaction()
	require.Equal(t, "val2", db.Get("key1"))
	db.Delete("key1")
	db.Set("key3", "val4")
	require.Equal(t, "", db.Get("key1"))
	require.Equal(t, "val4", db.Get("key3"))

	db.StartTransaction()
	db.Set("key1", "val5")
	db.Delete("key3")
	require.Equal(t, "val5", db.Get("key1"))
	require.Equal(t, "", db.Get("key3"))
	db.Rollback()

	require.Equal(t, "", db.Get("key1"))
	require.Equal(t, "val4", db.Get("key3"))

	db.Commit()
	require.Equal(t, "", db.Get("key1"))
	require.Equal(t, "val4", db.Get("key3"))

	db.Commit()
	require.Equal(t, "", db.Get("key1"))
	require.Equal(t, "val4", db.Get("key3"))
}

func TestRollbackDeeplyNestedTransaction_2(t *testing.T) {
	db := in_memory.InMemoryDatabase()

	db.Set("key1", "val1")
	require.Equal(t, "val1", db.Get("key1"))

	db.StartTransaction()
	db.Set("key1", "val2")
	db.Set("key3", "val3")
	require.Equal(t, "val2", db.Get("key1"))
	require.Equal(t, "val3", db.Get("key3"))

	db.StartTransaction()
	require.Equal(t, "val2", db.Get("key1"))
	db.Delete("key1")
	db.Set("key3", "val4")
	require.Equal(t, "", db.Get("key1"))
	require.Equal(t, "val4", db.Get("key3"))

	db.StartTransaction()
	db.Set("key1", "val5")
	db.Delete("key3")
	require.Equal(t, "val5", db.Get("key1"))
	require.Equal(t, "", db.Get("key3"))
	db.Commit()

	require.Equal(t, "val5", db.Get("key1"))
	require.Equal(t, "", db.Get("key3"))

	db.Rollback()
	require.Equal(t, "val2", db.Get("key1"))
	require.Equal(t, "val3", db.Get("key3"))

	db.Commit()
	require.Equal(t, "val2", db.Get("key1"))
	require.Equal(t, "val3", db.Get("key3"))
}
