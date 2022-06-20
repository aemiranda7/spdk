#ifndef LIBFOPS_H
#define LIBFOPS_H

/**
 * \file
 * \brief Implements simple fault injection operations.
 */

#include <unistd.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @enum BufferCorruptionPattern
 * @brief Types of corruption faults passed into the corruption function.
 **/
typedef enum {

    REPLACE_ALL_ZEROS, /**< Replaces all bytes with 0's (zero bits set) */
    REPLACE_ALL_ONES, /**< Replaces all bytes with 1's (all bits set) */
    BITFLIP_RANDOM_INDEX, /**< Flips a single bit in a random byte offset */
    BITFLIP_CUSTOM_INDEX /**< Flips a single bit in a custom bit index (0-7) byte offset (0-size) */

} BufferCorruptionPattern;


/**
* \brief Corrupts a given data buffer
* \param buffer The data buffer 
* \param size The size of the buffer in bytes
* \param corruptionPattern The way how the buffer will be corrupted. The different Patterns are spcified in the BufferCorruptionPattern Enum.
* \param customOffset Only needed for the BITFLIP_CUSTOM_INDEX pattern and specifies the index of the byte where the corruption will take place.
* It must be an integer between 0 and size-1.
* \param customIndex Only needed for the BITFLIP_CUSTOM_INDEX pattern and specifies the index of the bit where the corruption will take place.
* It must be an integer between 0 and 7.
*\return  0 if the corruption was made successfully;
*\return -1 if the corruption pattern provided is not correct;
*\return -2 if the customOffset provided is not between 0 and size-1 but just in the case where the corruption pattern is BITFLIP_CUSTOM_INDEX;
*\return -3 if the customIndex provided is not between 0 and 7 but just in the case where the corruption pattern is BITFLIP_CUSTOM_INDEX
*/
int corrupt_buffer (void *buffer, size_t size, BufferCorruptionPattern corruptionPattern, int customOffset, int customIndex);

/**
 * \brief Injects a delay of a given time. 
 * 
 * \param time The time to sleep
 * \returns 0 if delay was injected.
 **/
int operation_delay (double time);

/**
 * \brief Injects a given medium error.
 *        Custom errors are usually from <errno.h>, but any given integer is just returned. 
 * 
 * \param error_number 
 * \returns the requested error.
 **/
int medium_error (int error_number);

#ifdef __cplusplus
}
#endif

#endif /* LIBFOPS_H */