import random


class RandomProvider:

    def __init__(self, seed: int | None = None):
        self._random = random.Random(seed)

    def randint(self, a: int, b: int) -> int:
        return self._random.randint(a, b)

    def choice(self, sequence):
        return self._random.choice(sequence)

    def random(self) -> float:
        return self._random.random()

    def choices(self, population, *, weights=None, k=1):
        return self._random.choices(
            population,
            weights=weights,
            k=k,
        )

    def sample(self, population, *, k: int):
        return self._random.sample(
            population,
            k=k,
        )