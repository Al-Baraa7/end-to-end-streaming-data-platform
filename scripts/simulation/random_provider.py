"""RandomProvider — Phase 2: Identity & Randomness

Single shared RNG instance (seed=42) for bit-for-bit deterministic replay across all modules.
See README.md#phase-markers-used-in-code-comments
"""

import random
import math


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

    def uniform(self, a: float, b: float) -> float:
        return self._random.uniform(a, b)

    def gauss(self, mu: float, sigma: float) -> float:
        return self._random.gauss(mu, sigma)

    def poisson(self, lam: float) -> int:
        """
        Sample from Poisson(lam) - the expectation behind the event-volume model.

        Knuth's algorithm for lam < 30, normal approximation above it, so no upper
        cap is needed. lambda is deliberately never clipped: capping it would pull
        the realised mean below the video's declared latent popularity.
        """
        if lam <= 0:
            return 0

        if lam < 30:
            # Knuth's algorithm
            L = math.exp(-lam)
            k = 0
            p = 1.0
            while p > L:
                k += 1
                p *= self._random.random()
            return k - 1
        else:
            # Normal approximation for large lambda
            # Poisson(lam) ≈ Normal(lam, sqrt(lam))
            return max(0, int(self.gauss(lam, math.sqrt(lam)) + 0.5))