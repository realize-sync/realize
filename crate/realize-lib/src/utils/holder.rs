use crate::model::PathError;
use redb::Value;
use std::marker::PhantomData;
use std::str::Utf8Error;

/// A type that can be converted to and from bytes.
///
/// Either conversion are allowed to fail without causing a panic..
pub trait ByteConvertible<T> {
    fn from_bytes(data: &[u8]) -> Result<T, ByteConversionError>;
    fn to_bytes(&self) -> Result<Vec<u8>, ByteConversionError>;
}

/// Give a name to the type; used by redb.
pub trait NamedType {
    fn typename() -> &'static str;
}

/// A convenient type to use as value in redb tables.
///
/// All this type does is make parsing and generating [redb::Value]s
/// convenient and typesafe.
///
/// One big difference between this type and implementing
/// [redb::Value] directly is that serialization and deserialization
/// can fail without causing a panic.
///
/// To use a type in a holder, make it implement both [NamedType] and
/// [ByteConvertible].
#[derive(Clone, Debug)]
pub enum Holder<'a, T> {
    Borrowed(&'a [u8], PhantomData<T>),
    Owned(Vec<u8>, PhantomData<T>),
}

impl<'a, T> Holder<'a, T> {
    /// Return the data in the holder as a slice.
    pub fn as_bytes(&self) -> &'_ [u8] {
        match self {
            Holder::Owned(vec, _) => vec.as_slice(),
            Holder::Borrowed(arr, _) => arr,
        }
    }
}

impl<'a, T> Holder<'a, T>
where
    T: ByteConvertible<T>,
{
    /// Create a Holder containing the byte representation of the
    /// given instance.
    pub fn with_content(obj: T) -> Result<Self, ByteConversionError> {
        Self::new(&obj)
    }

    /// Create a Holder containing the byte representation of the
    /// given instance.
    pub fn new(obj: &T) -> Result<Self, ByteConversionError> {
        Ok(Holder::Owned(obj.to_bytes()?, PhantomData))
    }

    /// Converts the Holder into an instance of the expected type.
    pub fn parse(self) -> Result<T, ByteConversionError> {
        match self {
            Holder::Owned(vec, _) => T::from_bytes(vec.as_slice()),
            Holder::Borrowed(arr, _) => T::from_bytes(arr),
        }
    }
}

impl<'a, T> From<&'a [u8]> for Holder<'a, T> {
    fn from(arr: &'a [u8]) -> Self {
        Holder::Borrowed::<T>(arr, PhantomData)
    }
}

impl<T> Value for Holder<'_, T>
where
    T: NamedType + std::fmt::Debug,
{
    type SelfType<'a>
        = Holder<'a, T>
    where
        Self: 'a;

    type AsBytes<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        data.into()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.as_bytes()
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new(T::typename())
    }
}

/// Abstract the different serialization mechanisms used in this
/// project.
#[derive(Debug, thiserror::Error)]
pub enum ByteConversionError {
    #[error("capnp error {0}")]
    Capnp(#[from] capnp::Error),

    #[error("capnp error {0}")]
    CapnpNotInSchema(#[from] capnp::NotInSchema),

    #[error(transparent)]
    Path(#[from] PathError),

    #[error("invalid string: {0}")]
    Utf8(#[from] Utf8Error),

    #[error("invalid {0}")]
    Invalid(&'static str),
}
