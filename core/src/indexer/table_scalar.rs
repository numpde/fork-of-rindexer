//! The scalar conversions used by arithmetic writes and their inverse queries.
//!
//! Select a conversion once from the source/target types. Its Rust and SQL
//! implementations must describe the same stored value, including the signed
//! interpretation of uint256 and the existing BIGINT representation of uint64.
use alloy::{
    dyn_abi::DynSolValue,
    primitives::{Address, B256, I256, U256},
};
use chrono::DateTime;

use crate::{manifest::contract::ColumnType, EthereumSqlTypeWrapper};

#[derive(Clone, Copy)]
enum Source {
    Unsigned,
    Signed,
    Bool,
    Timestamp,
    String,
    Address,
    Bytes,
    Bytes32,
    Addresses,
}

#[derive(Clone, Copy)]
enum Conversion {
    Number,
    Boolean,
    DecimalString,
    String,
    Address,
    AddressString,
    LowAddress,
    Bytes32,
    HexString,
    Timestamp,
    Addresses,
}

pub(crate) struct ScalarConversion {
    source: Source,
    target: ColumnType,
    conversion: Conversion,
}

fn is_unsigned(kind: &ColumnType) -> bool {
    matches!(
        kind,
        ColumnType::Uint8
            | ColumnType::Uint16
            | ColumnType::Uint32
            | ColumnType::Uint64
            | ColumnType::Uint128
            | ColumnType::Uint256
    )
}

fn is_signed(kind: &ColumnType) -> bool {
    matches!(
        kind,
        ColumnType::Int8
            | ColumnType::Int16
            | ColumnType::Int32
            | ColumnType::Int64
            | ColumnType::Int128
            | ColumnType::Int256
    )
}

impl ScalarConversion {
    pub(crate) fn new(source: &ColumnType, target: &ColumnType) -> Result<Self, String> {
        let unsupported = || {
            format!(
            "unsupported arithmetic operand conversion from {} to {}; use a compatible typed event field or a scalar array element",
            source.to_type_string(), target.to_type_string()
        )
        };
        if (*source == ColumnType::Bytes
            && matches!(target, ColumnType::Bytes | ColumnType::Bytes32 | ColumnType::String))
            || (*source == ColumnType::Bytes32 && *target == ColumnType::Bytes)
        {
            return Err("byte arithmetic keys require an unambiguous conversion: legacy debug-byte values and fixed-bytes padding cannot be reconstructed from this source/target type pair; use an ABI bytes32 field with a bytes32/string key, or an ABI string field, and rebuild affected derived state".into());
        }
        let source = if is_unsigned(source) {
            Source::Unsigned
        } else if is_signed(source) {
            Source::Signed
        } else {
            match source {
                ColumnType::Bool => Source::Bool,
                ColumnType::Timestamp => Source::Timestamp,
                ColumnType::String => Source::String,
                ColumnType::Address => Source::Address,
                ColumnType::Bytes => Source::Bytes,
                ColumnType::Bytes32 => Source::Bytes32,
                ColumnType::Array(inner) if **inner == ColumnType::Address => Source::Addresses,
                _ => return Err(unsupported()),
            }
        };
        let conversion = if (is_unsigned(target) || is_signed(target))
            && matches!(
                source,
                Source::Unsigned | Source::Signed | Source::Bool | Source::Timestamp
            ) {
            Conversion::Number
        } else {
            match (source, target) {
                (Source::Unsigned | Source::Bool, ColumnType::Bool) => Conversion::Boolean,
                (Source::Unsigned | Source::Signed, ColumnType::String) => {
                    Conversion::DecimalString
                }
                (Source::String, ColumnType::String) => Conversion::String,
                (Source::Address, ColumnType::Address) => Conversion::Address,
                (Source::Address, ColumnType::String) => Conversion::AddressString,
                (Source::Unsigned, ColumnType::Address) => Conversion::LowAddress,
                (Source::Bytes32, ColumnType::Bytes32) => Conversion::Bytes32,
                (Source::Bytes32, ColumnType::String) => Conversion::HexString,
                (Source::Unsigned | Source::Timestamp, ColumnType::Timestamp) => {
                    Conversion::Timestamp
                }
                (Source::Addresses, ColumnType::Array(inner)) if **inner == ColumnType::Address => {
                    Conversion::Addresses
                }
                _ => return Err(unsupported()),
            }
        };
        Ok(Self { source, target: target.clone(), conversion })
    }

    /// Timestamp metadata is supplied as its exact unsigned epoch seconds;
    /// other values retain their ABI-decoded shape until conversion.
    pub(crate) fn normalize(&self, value: &DynSolValue) -> Result<EthereumSqlTypeWrapper, String> {
        use EthereumSqlTypeWrapper as Value;
        let invalid = || {
            format!(
                "required arithmetic operand is not representable as {}",
                self.target.to_type_string()
            )
        };
        match self.conversion {
            Conversion::Number => self.number(value),
            Conversion::Boolean => match value {
                DynSolValue::Uint(value, _) => Ok(Value::Bool(!value.is_zero())),
                DynSolValue::Bool(value) => Ok(Value::Bool(*value)),
                _ => Err(invalid()),
            },
            Conversion::DecimalString => match value {
                DynSolValue::Uint(value, _) => Ok(Value::String(value.to_string())),
                DynSolValue::Int(value, _) => Ok(Value::String(value.to_string())),
                _ => Err(invalid()),
            },
            Conversion::String => match value {
                DynSolValue::String(value) => Ok(Value::String(value.replace('\0', ""))),
                _ => Err(invalid()),
            },
            Conversion::Address | Conversion::AddressString => match value {
                DynSolValue::Address(value) => {
                    Ok(if matches!(self.conversion, Conversion::Address) {
                        Value::Address(*value)
                    } else {
                        Value::String(format!("{value:#x}"))
                    })
                }
                _ => Err(invalid()),
            },
            Conversion::LowAddress => match value {
                DynSolValue::Uint(value, _) => {
                    let bytes: [u8; 32] = value.to_be_bytes();
                    Ok(Value::Address(Address::from_slice(&bytes[12..])))
                }
                _ => Err(invalid()),
            },
            Conversion::Bytes32 | Conversion::HexString => {
                let bytes = match value {
                    DynSolValue::Bytes(value) => value.as_slice(),
                    DynSolValue::FixedBytes(value, length) => &value[..*length],
                    _ => return Err(invalid()),
                };
                Ok(match self.conversion {
                    Conversion::HexString => {
                        Value::String(format!("0x{}", alloy::hex::encode(bytes)))
                    }
                    _ => {
                        if bytes.len() != 32 {
                            return Err(invalid());
                        }
                        Value::B256(B256::from_slice(bytes))
                    }
                })
            }
            Conversion::Timestamp => {
                let DynSolValue::Uint(value, _) = value else { return Err(invalid()) };
                let seconds = i64::try_from(*value).map_err(|_| invalid())?;
                DateTime::from_timestamp(seconds, 0).map(Value::DateTime).ok_or_else(invalid)
            }
            Conversion::Addresses => {
                let (DynSolValue::Array(values) | DynSolValue::FixedArray(values)) = value else {
                    return Err(invalid());
                };
                // The existing VecAddress encoder represents an empty array as
                // SQL NULL. It cannot serve as a required arithmetic key.
                if values.is_empty() {
                    return Err(
                        "required arithmetic array key would encode as NULL; select a nonempty key"
                            .into(),
                    );
                }
                values
                    .iter()
                    .map(|value| match value {
                        DynSolValue::Address(value) => Ok(*value),
                        _ => Err(invalid()),
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map(Value::VecAddress)
            }
        }
    }

    fn number(&self, value: &DynSolValue) -> Result<EthereumSqlTypeWrapper, String> {
        use EthereumSqlTypeWrapper as Value;
        let invalid = || {
            format!(
                "required arithmetic operand is outside the {} input range",
                self.target.to_type_string()
            )
        };
        let (unsigned, signed) = match value {
            DynSolValue::Uint(value, _) => (Some(*value), None),
            DynSolValue::Int(value, _) => (None, Some(*value)),
            DynSolValue::Bool(value) => (Some(U256::from(u8::from(*value))), None),
            _ => return Err(invalid()),
        };
        if is_signed(&self.target) {
            let value = signed.unwrap_or_else(|| I256::from_raw(unsigned.expect("numeric input")));
            return Ok(match self.target {
                ColumnType::Int8 => Value::I8(i8::try_from(value).map_err(|_| invalid())?),
                ColumnType::Int16 => Value::I16(i16::try_from(value).map_err(|_| invalid())?),
                ColumnType::Int32 => Value::I32(i32::try_from(value).map_err(|_| invalid())?),
                ColumnType::Int64 => Value::I64(i64::try_from(value).map_err(|_| invalid())?),
                // Both existing wide signed custom-table types use I256/NUMERIC.
                _ => Value::I256Numeric(value),
            });
        }
        if matches!(self.target, ColumnType::Uint128 | ColumnType::Uint256) {
            return Ok(match (unsigned, signed) {
                (Some(value), _) => Value::U256Numeric(value),
                (_, Some(value)) => Value::I256Numeric(value),
                _ => unreachable!("numeric input"),
            });
        }
        let value = match (unsigned, signed) {
            (Some(value), _) => value,
            (_, Some(value)) if !value.is_negative() => value.into_raw(),
            _ => return Err(invalid()),
        };
        Ok(match self.target {
            ColumnType::Uint8 => Value::U8(u8::try_from(value).map_err(|_| invalid())?),
            ColumnType::Uint16 => Value::U16(u16::try_from(value).map_err(|_| invalid())?),
            ColumnType::Uint32 => Value::U32(u32::try_from(value).map_err(|_| invalid())?),
            ColumnType::Uint64 => Value::U64BigInt(u64::try_from(value).map_err(|_| invalid())?),
            _ => unreachable!("numeric target"),
        })
    }

    pub(crate) fn postgres_sql(&self, expression: String) -> String {
        let cast = |value: String| format!("CAST({value} AS {})", self.target.to_postgres_type());
        match self.conversion {
            Conversion::Number => {
                let value = match self.source {
                    Source::Bool => format!("(CASE WHEN {expression} IS NULL THEN NULL WHEN {expression} THEN 1 ELSE 0 END)"),
                    Source::Timestamp => format!("TRUNC(EXTRACT(EPOCH FROM {expression}))"),
                    _ => expression,
                };
                let value = if is_signed(&self.target) && matches!(self.source, Source::Unsigned) {
                    signed_bits(value, 256)
                } else if self.target == ColumnType::Uint64 {
                    // U64BigInt writes the low 64 bits through an i64 parameter.
                    signed_bits(value, 64)
                } else {
                    value
                };
                cast(value)
            }
            Conversion::Boolean if matches!(self.source, Source::Unsigned) => {
                format!("({expression} <> 0)")
            }
            Conversion::LowAddress => {
                // PostgreSQL to_hex accepts BIGINT. Split the low 160 bits into
                // exact numeric limbs before interpreting each 64-bit limb.
                let low = signed_bits(format!("MOD({expression}, 18446744073709551616)"), 64);
                let middle = signed_bits(
                    format!("MOD(DIV({expression}, 18446744073709551616), 18446744073709551616)"),
                    64,
                );
                let high = format!(
                    "MOD(DIV({expression}, 340282366920938463463374607431768211456), 4294967296)"
                );
                cast(format!("('0x' || LPAD(TO_HEX(CAST({high} AS BIGINT)), 8, '0') || LPAD(TO_HEX(CAST({middle} AS BIGINT)), 16, '0') || LPAD(TO_HEX(CAST({low} AS BIGINT)), 16, '0'))"))
            }
            Conversion::Bytes32 => {
                // B256's established custom-table encoder writes the ASCII
                // 0x-prefixed hex representation into BYTEA, unlike raw ABI
                // event storage, whose Bytes encoder writes 32 binary bytes.
                format!("CONVERT_TO('0x' || ENCODE({expression}, 'hex'), 'UTF8')")
            }
            Conversion::HexString => format!("('0x' || ENCODE({expression}, 'hex'))"),
            Conversion::Timestamp if matches!(self.source, Source::Unsigned) => {
                // Whole seconds can exceed f64's exact microsecond range. Split
                // the epoch into integer days and a remainder smaller than one
                // day; timestamp arithmetic stays exact and independent of DST.
                format!("((TIMESTAMP '1970-01-01' + MAKE_INTERVAL(days => CAST(DIV({expression}, 86400) AS INTEGER), secs => CAST(MOD({expression}, 86400) AS DOUBLE PRECISION))) AT TIME ZONE 'UTC')")
            }
            Conversion::Addresses => {
                format!("NULLIF({}, ARRAY[]::TEXT[])", cast(expression))
            }
            _ => cast(expression),
        }
    }
}

fn signed_bits(expression: String, bits: usize) -> String {
    let (sign, modulus) = match bits {
        64 => ("9223372036854775808", "18446744073709551616"),
        256 => (
            "57896044618658097711785492504343953926634992332820282019728792003956564819968",
            "115792089237316195423570985008687907853269984665640564039457584007913129639936",
        ),
        _ => unreachable!("supported signed representation"),
    };
    format!(
        "(CASE WHEN {expression} >= {sign} THEN {expression} - {modulus} ELSE {expression} END)"
    )
}
