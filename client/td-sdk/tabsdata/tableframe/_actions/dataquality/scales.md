# Identity Range

## Arguments

- `min`
- `max`
 
## Formula

$$
\text{bucket}(x) = x - \min
$$

---

## Tick

$$
\text1
$$

## Example

Mapping integers from `0` to `100` into `101` (`p0` to `p100`) buckets:

- `min = 0`
- `max = 100`

$$
\text{bucket}(x) = x
$$

| x   | bucket(x) |
|-----|-----------|
| 0   | 0         |
| 1   | 1         |
| 50  | 50        |
| 99  | 99        |
| 100 | 100       |

---

# Linear Range

## Arguments

- `min`
- `max`
- `q` (defaults to $\,\max - \min + 1\,$)

## Formula

$$
\text{bucket}(x)
=
\left\lfloor
q \cdot
\frac{x - \min}{\max - \min + 1}
\right\rfloor
$$

## Tick

$$
\text{tick} =
\frac{\max - \min + 1}{q}
$$

---

## Example

Mapping integers from `0` to `100` into `101` (`p0` to `p100`) buckets:

- `min = 0`
- `max = 100`
- `q = 101`

$$
\text{bucket}(x)
= x
$$

| x   | bucket(x) |
|-----|-----------|
| 0   | 0         |
| 1   | 1         |
| 50  | 50        |
| 99  | 99        |
| 100 | 100       |

---

## Example

Mapping integers from `10` to `20` into `2` buckets:

- `min = 10`
- `max = 20`
- `q = 2`

$$
\text{bucket}(x)
=
\left\lfloor
2 \cdot
\frac{x - 10}{11}
\right\rfloor
$$

Selected values:

| x  | bucket(x) |
|----|-----------|
| 10 | 0         |
| 11 | 0         |
| 12 | 0         |
| 18 | 2         |
| 19 | 2         |
| 20 | 2         |

---

## Example

Mapping integers from `10` to `20` into `6` buckets:

- `min = 10`
- `max = 20`
- `q = 6`

$$
\text{bucket}(x)
=
\left\lfloor
6 \cdot
\frac{x - 10}{11}
\right\rfloor
$$

Selected values:

| x   | bucket(x) |
|-----|-----------|
| 10  | 0         |
| 11  | 0         |
| 12  | 1         |
| 18  | 4         |
| 19  | 4         |
| 20  | 5         |

---

## Example

Mapping integers from `10` to `20`:

- `min = 10`
- `max = 20`
- `q = 11` (default)

$$
\text{bucket}(x) = x - 10
$$

Selected values:

| x   | bucket(x) |
|-----|-----------|
| 10  | 0         |
| 11  | 1         |
| 12  | 2         |
| 18  | 8         |
| 19  | 9         |
| 20  | 10        |

---

# Quadratic Range (Polynomial?)

## Arguments

- `min`
- `max`
- `q` (defaults to $\,\lfloor \sqrt{\max} \rfloor - \lfloor \sqrt{\min} \rfloor + 1,$ when `min` and `max` are perfect squares)

## Formula

$$
\text{bucket}(x)
=
\left\lfloor
q \cdot
\frac{\sqrt{x} - \sqrt{\min}}{\sqrt{\max} - \sqrt{\min} + 1}
\right\rfloor
$$

## Tick

$$
\text{tick} =
\frac{\sqrt{\max} - \sqrt{\min} + 1}{q}
$$

---

## Example

Mapping square integers from `4` to `75^2` into `30` buckets:

- `min = 4`
- `max = 75^2 = 5625`
- `q = 30`

$$
\sqrt{\min} = \sqrt{4} = 2,\qquad \sqrt{\max} = \sqrt{5625} = 75
$$

$$
\text{bucket}(x)
=
\left\lfloor
30 \cdot
\frac{\sqrt{x} - 2}{75 - 2 + 1}
\right\rfloor
=
\left\lfloor
30 \cdot
\frac{\sqrt{x} - 2}{74}
\right\rfloor
$$

Selected values (using squares):

| x      | √x | bucket(x) |
|--------|----|-----------|
| 4      | 2  | 0         |
| 9      | 3  | 0         |
| 16     | 4  | 0         |
| 100    | 10 | 3         |
| 400    | 20 | 7         |
| 2500   | 50 | 19        |
| 5625   | 75 | 29        |

---

## Example

Mapping square integers from `1^2` to `10^2` into `10` buckets:

- `min = 1`
- `max = 100`
- `q = 10`

$$
\sqrt{\min}=1,\qquad \sqrt{\max}=10
$$

$$
\text{bucket}(x)
=
\left\lfloor
10 \cdot
\frac{\sqrt{x} - 1}{10 - 1 + 1}
\right\rfloor
=
\left\lfloor
10 \cdot
\frac{\sqrt{x} - 1}{10}
\right\rfloor
$$

Selected values:

| x   | √x | bucket(x) |
|-----|----|-----------|
| 1   | 1  | 0         |
| 4   | 2  | 1         |
| 9   | 3  | 2         |
| 16  | 4  | 3         |
| 25  | 5  | 4         |
| 36  | 6  | 5         |
| 49  | 7  | 6         |
| 64  | 8  | 7         |
| 81  | 9  | 8         |
| 100 | 10 | 9         |

---

# Exponential Range

## Arguments

- `min`
- `max`
- `q` (defaults to $\,\lfloor \log(\max) \rfloor - \lfloor \log(\min) \rfloor + 1\)
- `base` (optional, defaults to $e$)

---

## Formula

$$
\text{bucket}(x)
=
\left\lfloor
q \cdot
\frac{\log_b(x) - \log_b(\min)}
{\log_b(\max) - \log_b(\min) + 1}
\right\rfloor
$$

## Tick

$$
\text{tick}
=
\frac{\log_b(\max) - \log_b(\min) + 1}{q}
$$

Note:
- `tick` is a **real-valued quantity**
- it is **derived**, not an independent parameter
- flooring is applied only when computing the bucket index

---



---

## Example

Mapping exponential values from `1` to `10^6` into `6` buckets
(using base 10 logarithms):

- `min = 1`
- `max = 10^6`
- `q = 6`
- `base = 10`


$$
\log_{10}(1) = 0,\qquad \log_{10}(10^6) = 6
$$

$$
\text{bucket}(x)
=
\left\lfloor
6 \cdot
\frac{\log_{10}(x)}{7}
\right\rfloor
$$

| x         | log₁₀(x) | bucket(x) |
|-----------|----------|-----------|
| 1         | 0        | 0         |
| 10        | 1        | 0         |
| 100       | 2        | 1         |
| 1,000     | 3        | 2         |
| 10,000    | 4        | 3         |
| 100,000   | 5        | 4         |
| 1,000,000 | 6        | 5         |

---

## Example

Mapping powers of 2 from `2^1` to `2^8` into `8` buckets
(using base 2 logarithms):

- `min = 2`
- `max = 256`
- `q = 8`
- `base = 2`

$$
\log_2(\min) = 1,\qquad \log_2(\max) = 8
$$

$$
\text{bucket}(x)
=
\left\lfloor
8 \cdot
\frac{\log_2(x) - 1}{8}
\right\rfloor
=
\left\lfloor
\log_2(x) - 1
\right\rfloor
$$

| x   | log₂(x) | bucket(x) |
|-----|---------|-----------|
| 2   | 1       | 0         |
| 4   | 2       | 1         |
| 8   | 3       | 2         |
| 16  | 4       | 3         |
| 32  | 5       | 4         |
| 64  | 6       | 5         |
| 128 | 7       | 6         |
| 256 | 8       | 7         |

---

# Logarithmic Range

---

## Arguments

- `min`
- `max`
- `q` (defaults to $\,\lfloor \exp(\max) \rfloor - \lfloor \exp(\min) \rfloor + 1\,$)
- `base` (optional, default: $e$)

---

## Formula

$$
\text{bucket}(x)
=
\left\lfloor
q \cdot
\frac{b^{x} - b^{\min}}
{b^{\max} - b^{\min} + 1}
\right\rfloor
$$

---

## Tick

$$
\text{tick}
=
\frac{b^{\max} - b^{\min} + 1}{q}
$$

---

## Example

Mapping logarithmic values from `0` to `6` into `6` buckets
(using base 10):

- `min = 0`
- `max = 6`
- `q = 6`
- `base = 10`

$$
10^{0} = 1,\qquad 10^{6} = 1{,}000{,}000
$$

$$
\text{bucket}(x)
=
\left\lfloor
6 \cdot
\frac{10^{x} - 1}{1{,}000{,}000}
\right\rfloor
$$

Selected values:

| x | 10ˣ       | bucket(x) |
|---|-----------|-----------|
| 0 | 1         | 0         |
| 1 | 10        | 0         |
| 2 | 100       | 0         |
| 3 | 1,000     | 0         |
| 4 | 10,000    | 0         |
| 5 | 100,000   | 0         |
| 6 | 1,000,000 | 5         |

---

## Example

Mapping logarithmic values from `1` to `4` into `4` buckets
(using base 2):

- `min = 1`
- `max = 4`
- `q = 4`
- `base = 2`

$$
2^{\min} = 2,\qquad 2^{\max} = 16
$$

$$
\text{bucket}(x)
=
\left\lfloor
4 \cdot
\frac{2^{x} - 2}{15}
\right\rfloor
$$

Selected values:

| x | 2ˣ | bucket(x) |
|---|----|-----------|
| 1 | 2  | 0         |
| 2 | 4  | 0         |
| 3 | 8  | 1         |
| 4 | 16 | 3         |

---

---

Note: `⌊ ⌋` stands for the floor operation.