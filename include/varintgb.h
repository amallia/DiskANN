#pragma once

#include <array>
#include <cstring>
#include <vector>

template <typename T, typename Byte>
class ReinterpretProxy {
    static_assert(std::is_trivially_constructible_v<T>);
    Byte* m_ptr;
    std::size_t m_len;

  public:
    explicit ReinterpretProxy(Byte* ptr, std::size_t len = sizeof(T)) : m_ptr(ptr), m_len(len) {}

    void operator=(T const& value) { std::memcpy(m_ptr, &value, m_len); }

    [[nodiscard]] auto operator*() const -> T
    {
        T dst{0};
        std::memcpy(&dst, m_ptr, m_len);
        return dst;
    }
};
template <typename T>
auto bitwise_reinterpret(std::uint8_t* dst) -> ReinterpretProxy<T, std::uint8_t>
{
    return ReinterpretProxy<T, std::uint8_t>{dst, sizeof(T)};
}

template <typename T>
auto bitwise_reinterpret(std::uint8_t const* dst, std::size_t len = sizeof(T))
    -> ReinterpretProxy<T, std::uint8_t const>
{
    return ReinterpretProxy<T, std::uint8_t const>{dst, len};
}


namespace diskann {

template <bool delta = false>
class VarIntGB {
  public:
    size_t encodeArray(const uint32_t* in, const size_t length, uint8_t* out)
    {
        uint32_t prev = 0;  // for delta
        const uint8_t* const initbout = out;

        size_t k = 0;
        for (; k + 3 < length; k += 4) {
            uint8_t* keyp = out++;
            *keyp = 0;
            {
                const uint32_t val = delta ? in[k] - prev : in[k];
                if (delta) {
                    prev = in[k];
                }
                if (val < (1U << 8)) {
                    *out++ = static_cast<uint8_t>(val);
                } else if (val < (1U << 16)) {
                    *out++ = static_cast<uint8_t>(val);
                    *out++ = static_cast<uint8_t>(val >> 8);
                    *keyp = static_cast<uint8_t>(1);
                } else if (val < (1U << 24)) {
                    *out++ = static_cast<uint8_t>(val);
                    *out++ = static_cast<uint8_t>(val >> 8);
                    *out++ = static_cast<uint8_t>(val >> 16);
                    *keyp = static_cast<uint8_t>(2);
                } else {
                    bitwise_reinterpret<uint32_t>(out) = val;
                    out += 4;
                    *keyp = static_cast<uint8_t>(3);
                }
            }
            {
                const uint32_t val = delta ? in[k + 1] - prev : in[k + 1];
                if (delta) {
                    prev = in[k + 1];
                }
                if (val < (1U << 8)) {
                    *out++ = static_cast<uint8_t>(val);
                } else if (val < (1U << 16)) {
                    *out++ = static_cast<uint8_t>(val);
                    *out++ = static_cast<uint8_t>(val >> 8);
                    *keyp |= static_cast<uint8_t>(1 << 2);
                } else if (val < (1U << 24)) {
                    *out++ = static_cast<uint8_t>(val);
                    *out++ = static_cast<uint8_t>(val >> 8);
                    *out++ = static_cast<uint8_t>(val >> 16);
                    *keyp |= static_cast<uint8_t>(2 << 2);
                } else {
                    bitwise_reinterpret<uint32_t>(out) = val;
                    out += 4;
                    *keyp |= static_cast<uint8_t>(3 << 2);
                }
            }
            {
                const uint32_t val = delta ? in[k + 2] - prev : in[k + 2];
                if (delta) {
                    prev = in[k + 2];
                }
                if (val < (1U << 8)) {
                    *out++ = static_cast<uint8_t>(val);
                } else if (val < (1U << 16)) {
                    *out++ = static_cast<uint8_t>(val);
                    *out++ = static_cast<uint8_t>(val >> 8);
                    *keyp |= static_cast<uint8_t>(1 << 4);
                } else if (val < (1U << 24)) {
                    *out++ = static_cast<uint8_t>(val);
                    *out++ = static_cast<uint8_t>(val >> 8);
                    *out++ = static_cast<uint8_t>(val >> 16);
                    *keyp |= static_cast<uint8_t>(2 << 4);
                } else {
                    bitwise_reinterpret<uint32_t>(out) = val;
                    out += 4;
                    *keyp |= static_cast<uint8_t>(3 << 4);
                }
            }
            {
                const uint32_t val = delta ? in[k + 3] - prev : in[k + 3];
                if (delta) {
                    prev = in[k + 3];
                }
                if (val < (1U << 8)) {
                    *out++ = static_cast<uint8_t>(val);
                } else if (val < (1U << 16)) {
                    *out++ = static_cast<uint8_t>(val);
                    *out++ = static_cast<uint8_t>(val >> 8);
                    *keyp |= static_cast<uint8_t>(1 << 6);
                } else if (val < (1U << 24)) {
                    *out++ = static_cast<uint8_t>(val);
                    *out++ = static_cast<uint8_t>(val >> 8);
                    *out++ = static_cast<uint8_t>(val >> 16);
                    *keyp |= static_cast<uint8_t>(2 << 6);
                } else {
                    bitwise_reinterpret<uint32_t>(out) = val;
                    out += 4;
                    *keyp |= static_cast<uint8_t>(3 << 6);
                }
            }
        }

        if (k < length) {
            uint8_t* keyp = out++;
            *keyp = 0;
            for (int j = 0; k < length && j < 8; j += 2, ++k) {
                const uint32_t val = delta ? in[k] - prev : in[k];
                if (delta) {
                    prev = in[k];
                }
                if (val < (1U << 8)) {
                    *out++ = static_cast<uint8_t>(val);
                } else if (val < (1U << 16)) {
                    *out++ = static_cast<uint8_t>(val);
                    *out++ = static_cast<uint8_t>(val >> 8);
                    *keyp |= static_cast<uint8_t>(1 << j);
                } else if (val < (1U << 24)) {
                    *out++ = static_cast<uint8_t>(val);
                    *out++ = static_cast<uint8_t>(val >> 8);
                    *out++ = static_cast<uint8_t>(val >> 16);
                    *keyp |= static_cast<uint8_t>(2 << j);
                } else {
                    // the compiler will do the right thing
                    *reinterpret_cast<uint32_t*>(out) = val;
                    out += 4;
                    *keyp |= static_cast<uint8_t>(3 << j);
                }
            }
        }
        const size_t storageinbytes = out - initbout;
        return storageinbytes;
    }

    size_t decodeArray(const uint8_t* in, const size_t n, uint32_t* out)
    {
        uint32_t prev = 0;  // for delta
        const uint8_t* initin = in;
        uint32_t val;
        size_t k = 0;
        while (k + 3 < n) {
            in = delta ? decodeGroupVarIntDelta(in, &prev, out) : decodeGroupVarInt(in, out);
            out += 4;
            k += 4;
        }
        while (k < n) {
            uint8_t key = *in++;
            for (int j = 0; k < n and j < 4; j++) {
                const uint32_t howmanybyte = key & 3;
                key = static_cast<uint8_t>(key >> 2);
                val = static_cast<uint32_t>(*in++);
                if (howmanybyte >= 1) {
                    val |= (static_cast<uint32_t>(*in++) << 8);
                    if (howmanybyte >= 2) {
                        val |= (static_cast<uint32_t>(*in++) << 16);
                        if (howmanybyte >= 3) {
                            val |= (static_cast<uint32_t>(*in++) << 24);
                        }
                    }
                }
                prev = (delta ? prev : 0) + val;
                *out++ = prev;
                k++;
            }
        }
        return in - initin;
    }

  protected:
    const uint8_t* decodeGroupVarInt(const uint8_t* in, uint32_t* out)
    {
        const uint32_t sel = *in++;

        if (sel == 0) {
            out[0] = static_cast<uint32_t>(in[0]);
            out[1] = static_cast<uint32_t>(in[1]);
            out[2] = static_cast<uint32_t>(in[2]);
            out[3] = static_cast<uint32_t>(in[3]);
            return in + 4;
        }
        const uint32_t sel1 = (sel & 3);
        *out++ = *bitwise_reinterpret<std::uint32_t>(in, sel1 + 1);
        in += sel1 + 1;
        const uint32_t sel2 = ((sel >> 2) & 3);
        *out++ = *bitwise_reinterpret<std::uint32_t>(in, sel2 + 1);
        in += sel2 + 1;
        const uint32_t sel3 = ((sel >> 4) & 3);
        *out++ = *bitwise_reinterpret<std::uint32_t>(in, sel3 + 1);
        in += sel3 + 1;
        const uint32_t sel4 = (sel >> 6);
        *out++ = *bitwise_reinterpret<std::uint32_t>(in, sel4 + 1);
        in += sel4 + 1;
        return in;
    }

    const uint8_t* decodeGroupVarIntDelta(const uint8_t* in, uint32_t* val, uint32_t* out)
    {
        const uint32_t sel = *in++;
        if (sel == 0) {
            out[0] = (*val += static_cast<uint32_t>(in[0]));
            out[1] = (*val += static_cast<uint32_t>(in[1]));
            out[2] = (*val += static_cast<uint32_t>(in[2]));
            out[3] = (*val += static_cast<uint32_t>(in[3]));
            return in + 4;
        }
        const uint32_t sel1 = (sel & 3);
        *val += *bitwise_reinterpret<std::uint32_t>(in, sel1 + 1);
        *out++ = *val;
        in += sel1 + 1;
        const uint32_t sel2 = ((sel >> 2) & 3);
        *val += *bitwise_reinterpret<std::uint32_t>(in, sel2 + 1);
        *out++ = *val;
        in += sel2 + 1;
        const uint32_t sel3 = ((sel >> 4) & 3);
        *val += *bitwise_reinterpret<std::uint32_t>(in, sel3 + 1);
        *out++ = *val;
        in += sel3 + 1;
        const uint32_t sel4 = (sel >> 6);
        *val += *bitwise_reinterpret<std::uint32_t>(in, sel4 + 1);
        *out++ = *val;
        in += sel4 + 1;
        return in;
    }
};


}  // namespace diskann