use anyhow::{anyhow, Context, Result};
use chrono::{Duration, NaiveDate, NaiveDateTime};

pub fn ts_to_date(ts_local_us: i64) -> Result<NaiveDate> {
    let seconds = ts_local_us / 1_000_000;
    let micros = ts_local_us % 1_000_000;
    let nanos = (micros as i64 * 1_000) as u32;
    let dt = NaiveDateTime::from_timestamp_opt(seconds, nanos)
        .ok_or_else(|| anyhow!("invalid ts_local_us: {}", ts_local_us))?;
    Ok(dt.date())
}

pub fn parse_date_opt(value: Option<&str>) -> Result<Option<NaiveDate>> {
    value
        .map(|val| {
            NaiveDate::parse_from_str(val, "%Y-%m-%d")
                .with_context(|| format!("invalid date string: {}", val))
        })
        .transpose()
}

pub fn date_to_days(date: NaiveDate) -> i32 {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    (date - epoch).num_days() as i32
}

pub fn days_to_date(days: i32) -> Result<NaiveDate> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    epoch
        .checked_add_signed(Duration::days(days as i64))
        .ok_or_else(|| anyhow!("invalid date days: {}", days))
}

pub fn date_to_ts_local_us(date: NaiveDate, end_of_day: bool) -> i64 {
    let dt = if end_of_day {
        date.and_hms_micro_opt(23, 59, 59, 999_999)
    } else {
        date.and_hms_micro_opt(0, 0, 0, 0)
    }
    .expect("valid date time");
    dt.timestamp_micros()
}

pub fn escape_sql_string(value: &str) -> String {
    value.replace('"', "''")
}
