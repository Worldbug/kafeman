package utils

func BatchesFromSlice[S any](slice []S, maxBatchSize int) [][]S {
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

func SliceToMap[T1 any, T2 comparable](items []T1, by func(item T1) T2) map[T2]T1 {
	result := make(map[T2]T1, len(items))
	for _, item := range items {
		result[by(item)] = item
	}

	return result
}

func MapToSlice[T1 any, T2 comparable](items map[T2]T1) []T1 {
	result := make([]T1, 0, len(items))
	for _, v := range items {
		result = append(result, v)
	}

	return result
}

func OrDefault[T comparable](t1, t2 T) T {
	empty := new(T)
	if t1 == *empty {
		return t2
	}

	return t1
}

func OrDefaultSlice[T comparable](t1, t2 []T) []T {
	if len(t1) == 0 {
		return t2
	}

	return t1
}
