# Using dcm4che

`dcm4che` is a very complete library implemented in Java to work with DICOM files.
Unfortunately, it is poorly documented.

## Basics

You need to read a file using a `DicomInputStream` (`org.dcm4che3.io.DicomInputStream`).
You can then get either a `DicomObject`, or focus on the `Attributes` (`org.dcm4che3.data.Attributes`).

To get the `Attributes`:

```scala
(dicomInputStream: DicomInputStream): Attributes => dicomInputStream.readDataset

// without loading the (sometime huge) pixel data
(dicomInputStream: DicomInputStream): Attributes => dicomInputStream.readDatasetUntilPixelData
```

An object `Attributes` is basically a list of "attributes", each having

- a tag: a unique identifier in the file, consider it the "key" but it's an `Int`
- a "VR": a Value Representation, consider it the "type" as per DICOM standard
- a value: an arbitrary value stored as bytes in the file

## FAQ

### What are the tags ?

All standard tags have a static variable defined in the `Tag` class (`org.dcm4che3.data.Tag`).

A tag is a concatenation of two `Short` (2 bytes) into one `Int` (4 bytes).
The first two bytes represent the "group number", and the last two bytes represents the "element number".

### How can I know the VR of a Tag? The keyword of a Tag?

Use the standard `ElementDictionary` (org.dcm4che3.data.ElementDictionary)

```scala
val stdElementDict = ElementDictionary.getStandardElementDictionary

(tag: Int): VR => stdElementDict.vrOf(tag)
(tag: Int): String => stdElementDict.keywordOf(tag)
```

You can find other useful functions in `ElementDictionary`

## Notes

### Set a date tag in Attributes

Basically, don't try to set an attribute value which is supposed to be a `VR.DA` using `Attributes.setDate`.
It will fail silently.

```scala
(attrs: Attributes, date: java.util.Date) => {
    // fails silently!
    attrs.setDate(Tag.StudyDate, date)
}
```

Instead, find the corresponding tag with a `long` signature (e.g. `Tag.StudyDate` corresponding tag is `Tag.StudyDateAndTime`).
Note however that it also sets the value for the associated `Time` tag (e.g. `Tag.StudyDateAndTime` will also set value for `Tag.StudyTime`).

```scala
(attrs: Attributes, date: java.util.Date) => {
    // works as intended
    attrs.setDate(Tag.StudyDateAndTime, date)
}
```

> This is because of a bug, where the int tag gets casted to long silently and will make the setDate function not behave as intended.
> https://github.com/dcm4che/dcm4che/blob/1e6d19a2634e16e40ed90b4ffc9a57c9d1ffbe5e/dcm4che-core/src/main/java/org/dcm4che3/data/Attributes.java#L2056-L2062
