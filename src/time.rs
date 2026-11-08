//! Timestamps for log records and ledger entries.
//!
//! A date-library dependency is not worth it for two call sites, so the civil
//! date conversion is inlined and unit tested.

use std::time::{SystemTime, UNIX_EPOCH};

/// Formats a system time as an RFC 3339 UTC timestamp with milliseconds.
pub fn format_rfc3339(time: SystemTime) -> String {
    let (seconds, millis) = match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => (duration.as_secs() as i64, duration.subsec_millis()),
        Err(error) => {
            let duration = error.duration();
            (-(duration.as_secs() as i64), 0)
        }
    };
    let days = seconds.div_euclid(86_400);
    let seconds_of_day = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;

    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}Z")
}

/// The current time as an RFC 3339 UTC timestamp.
pub fn now_rfc3339() -> String {
    format_rfc3339(SystemTime::now())
}

/// Milliseconds since the Unix epoch, used for monotonic-looking identifiers.
pub fn unix_millis(time: SystemTime) -> u128 {
    time.duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

/// Converts days since 1970-01-01 to a civil (year, month, day) date.
///
/// This is Howard Hinnant's `civil_from_days`.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_position = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * month_position + 2) / 5 + 1) as u32;
    let month = if month_position < 10 {
        month_position + 3
    } else {
        month_position - 9
    } as u32;
    if month <= 2 {
        year += 1;
    }
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn formats_the_epoch() {
        assert_eq!(format_rfc3339(UNIX_EPOCH), "1970-01-01T00:00:00.000Z");
    }

    #[test]
    fn formats_a_later_timestamp() {
        let time = UNIX_EPOCH + Duration::from_secs(1_800_000_000);
        assert_eq!(format_rfc3339(time), "2027-01-15T08:00:00.000Z");
    }

    #[test]
    fn formats_milliseconds_and_leap_days() {
        let time = UNIX_EPOCH + Duration::from_millis(951_782_400_123);
        assert_eq!(format_rfc3339(time), "2000-02-29T00:00:00.123Z");
    }

    #[test]
    fn unix_millis_survives_a_clock_before_the_epoch() {
        let time = UNIX_EPOCH - Duration::from_secs(1);
        assert_eq!(unix_millis(time), 0);
    }
}
