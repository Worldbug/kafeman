package kafeman

import "encoding/json"

func isJSON(data []byte) bool {
	var i interface{}
	if err := json.Unmarshal(data, &i); err == nil {
		return true
	}
	return false
}

func batchesFromSlice[S any](slice []S, maxBatchSize int) [][]S {
	batches := make([][]S, 0)

	for i := 0; i < len(slice); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(slice) {
			end = len(slice)
		}

		part := slice[i:end]
		batches = append(batches, part)
	}

	return batches
}
