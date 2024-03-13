"""Base async API utilities."""

import time
import asyncio


class TokenBucketError(Exception):
    pass


class TokenBucket:

    def __init__(self, max_bucket_size: int, rate: float) -> None:
        if max_bucket_size <= 0 or not isinstance(max_bucket_size, int):
            raise TokenBucketError("Max bucket size must be a positive integer.")
        self.max_bucket_size = max_bucket_size

        if rate <= 0:
            raise TokenBucketError("Rate must be a positive number.")
        self.rate = rate

        self.current_bucket_size = max_bucket_size
        self.bucket_resized_at = None

    def get_current_time_in_nanoseconds(self) -> int:
        return time.time_ns()
    
    def resize_bucket(self) -> None:
        current_time = self.get_current_time_in_nanoseconds()

        if self.bucket_resized_at is None:
            # first resize of bucket; set time and return
            self.bucket_resized_at = current_time
            return

        time_span = current_time - self.bucket_resized_at
        new_tokens = time_span * self.rate / 10**9
        combined_size = self.current_bucket_size + new_tokens
        self.current_bucket_size = min(combined_size, self.max_bucket_size)

    async def throttle(self) -> None:
        while self.current_bucket_size < 1:
            rate_per_token_in_seconds = 1 / self.rate
            min_tokens_required = 1 - self.current_bucket_size
            snooze_time = rate_per_token_in_seconds * min_tokens_required
            
            await asyncio.sleep(snooze_time)
            
            # update bucket to add tokens generated in the awaited time
            self.resize_bucket()

    async def __aenter__(self) -> None:
        self.resize_bucket()
        await self.throttle()
        self.current_bucket_size -= 1

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        # no control on exit
        pass
