/*
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package parser

import (
	"io"
	"strconv"
	"unicode/utf8"
)

const (
	YYEOF         = -1    /** This character denotes the end of file */
	ZZ_BUFFERSIZE = 16384 /** initial size of the lookahead buffer */
	/** lexical states */
	YYINITIAL = 0
	xc        = 2
	xq        = 4
	xdq       = 6
	xsb       = 8
	xbin      = 10
	xhex      = 12
	xhint     = 14
	xq2       = 16
	xq2_2     = 18
)

/**
* ZZ_LEXSTATE[l] is the state in the DFA for the lexical state l
* ZZ_LEXSTATE[l+1] is the state in the DFA for the lexical state l
*                  at the beginning of a line
* l is of the form l = 2*k, k a non negative integer
 */
var ZZ_LEXSTATE []int = []int{0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 4, 4, 7, 7, 8, 8}

/**
* Translates characters to character classes
 */
var ZZ_CMAP_PACKED []rune = []rune{
	0o011, 0o000, 0o001, 0o026, 0o001, 0o025, 0o001, 0o030, 0o001, 0o026, 0o001, 0o025, 0o022, 0o000, 0o001, 0o026, 0o001, 0o017, 0o001, 0o002,
	0o002, 0o012, 0o002, 0o017, 0o001, 0o001, 0o002, 0o017, 0o001, 0o004, 0o001, 0o023, 0o001, 0o017, 0o001, 0o027, 0o001, 0o016, 0o001, 0o003,
	0o001, 0o020, 0o011, 0o013, 0o001, 0o014, 0o001, 0o017, 0o001, 0o017, 0o001, 0o015, 0o003, 0o017, 0o001, 0o021, 0o001, 0o010, 0o001, 0o021,
	0o001, 0o024, 0o001, 0o022, 0o001, 0o024, 0o002, 0o012, 0o001, 0o034, 0o002, 0o012, 0o001, 0o033, 0o001, 0o012, 0o001, 0o031, 0o001, 0o036,
	0o001, 0o012, 0o001, 0o007, 0o001, 0o012, 0o001, 0o035, 0o001, 0o037, 0o001, 0o032, 0o002, 0o012, 0o001, 0o011, 0o002, 0o012, 0o001, 0o005,
	0o001, 0o000, 0o001, 0o006, 0o001, 0o017, 0o001, 0o012, 0o001, 0o000, 0o001, 0o021, 0o001, 0o010, 0o001, 0o021, 0o001, 0o024, 0o001, 0o022,
	0o001, 0o024, 0o002, 0o012, 0o001, 0o034, 0o002, 0o012, 0o001, 0o033, 0o001, 0o012, 0o001, 0o031, 0o001, 0o036, 0o001, 0o012, 0o001, 0o007,
	0o001, 0o012, 0o001, 0o035, 0o001, 0o037, 0o001, 0o032, 0o002, 0o012, 0o001, 0o011, 0o002, 0o012, 0o001, 0o017, 0o001, 0o017, 0o002, 0o017,
	0o001, 0o000, 0o005, 0o012, 0o001, 0o012, 0o172, 0o012, 0x1f28, 0o000, 0o001, 0o030, 0o001, 0o030, 0xffff, 0o000, 0xffff, 0o000, 0xffff, 0o000, 0xffff, 0o000, 0xffff, 0o000, 0xffff, 0o000, 0xffff, 0o000, 0xffff, 0o000, 0xffff, 0o000, 0xffff, 0o000, 0xffff, 0o000, 0xffff, 0o000, 0xffff, 0o000, 0xffff, 0o000, 0xffff, 0o000, 0xffff, 0o000, 0xdfe6, 0o000,
}

/**
* Translates characters to character classes
 */
var ZZ_CMAP = zzUnpackCMap(ZZ_CMAP_PACKED)

/**
* Translates DFA states to action switch labels.
 */
var ZZ_ACTION = zzUnpackActionNoParams()

var ZZ_ACTION_PACKED_0 []rune = []rune{
	0o011, 0o000, 0o001, 0o001, 0o001, 0o002, 0o001, 0o003, 0o002, 0o004, 0o004, 0o005, 0o001, 0o006, 0o002, 0o004,
	0o001, 0o006, 0o001, 0o007, 0o001, 0o004, 0o002, 0o005, 0o001, 0o010, 0o002, 0o011, 0o001, 0o012, 0o001, 0o013,
	0o001, 0o014, 0o001, 0o015, 0o001, 0o016, 0o001, 0o017, 0o001, 0o020, 0o001, 0o021, 0o001, 0o022, 0o001, 0o023,
	0o001, 0o024, 0o001, 0o025, 0o001, 0o026, 0o001, 0o007, 0o001, 0o027, 0o001, 0o000, 0o001, 0o030, 0o001, 0o031,
	0o001, 0o032, 0o001, 0o000, 0o001, 0o033, 0o001, 0o034, 0o001, 0o035, 0o001, 0o032, 0o001, 0o036, 0o001, 0o000,
	0o003, 0o005, 0o001, 0o037, 0o001, 0o040, 0o001, 0o000, 0o001, 0o041, 0o002, 0o000, 0o001, 0o042, 0o004, 0o000,
	0o001, 0o043, 0o001, 0o044, 0o001, 0o033, 0o001, 0o000, 0o001, 0o045, 0o002, 0o005, 0o003, 0o000, 0o001, 0o046,
	0o001, 0o047, 0o001, 0o050, 0o001, 0o051, 0o020, 0o000, 0o001, 0o052, 0o001, 0o000, 0o001, 0o053, 0o001, 0o052,
	0o001, 0o053,
}

func zzUnpackActionNoParams() []int {
	result := make([]int, 104)
	offset := 0
	offset = zzUnpackAction(ZZ_ACTION_PACKED_0, offset, result)
	return result
}

func zzUnpackAction(packed []rune, offset int, result []int) int {
	i := 0           /* index in packed string  */
	j := offset      /* index in unpacked array */
	l := len(packed) // 130
	for i < l {
		count := packed[i]
		i++
		value := packed[i]
		i++
		result[j] = int(value)
		j++
		count--
		for count > 0 {
			result[j] = int(value)
			j++
			count--
		}
	}
	return j
}

/**
* Translates a state to a row index in the transition table
 */
var ZZ_ROWMAP = zzUnpackRowMapNoParams()

var ZZ_ROWMAP_PACKED_0 []rune = []rune{
	0o000, 0o000, 0o000, 0o040, 0o000, 0o100, 0o000, 0o140, 0o000, 0o200, 0o000, 0o240, 0o000, 0o300, 0o000, 0o340,
	0o000, 0x0100, 0o000, 0o200, 0o000, 0o200, 0o000, 0o200, 0o000, 0x0120, 0o000, 0o200, 0o000, 0x0140, 0o000, 0x0160,
	0o000, 0x0180, 0o000, 0x01a0, 0o000, 0x01c0, 0o000, 0x01e0, 0o000, 0x0200, 0o000, 0x0220, 0o000, 0o200, 0o000, 0x0240,
	0o000, 0x0260, 0o000, 0x0280, 0o000, 0x02a0, 0o000, 0x02c0, 0o000, 0x02e0, 0o000, 0x0300, 0o000, 0x0320, 0o000, 0x0340,
	0o000, 0x0360, 0o000, 0x0380, 0o000, 0x03a0, 0o000, 0x03c0, 0o000, 0x03e0, 0o000, 0x0400, 0o000, 0o200, 0o000, 0o200,
	0o000, 0o200, 0o000, 0o200, 0o000, 0x0420, 0o000, 0o200, 0o000, 0x0440, 0o000, 0o200, 0o000, 0o200, 0o000, 0x0460,
	0o000, 0x0480, 0o000, 0o200, 0o000, 0o200, 0o000, 0o200, 0o000, 0x04a0, 0o000, 0o200, 0o000, 0x04c0, 0o000, 0x04e0,
	0o000, 0x0500, 0o000, 0x0520, 0o000, 0o200, 0o000, 0o200, 0o000, 0x02e0, 0o000, 0o200, 0o000, 0x0540, 0o000, 0x0560,
	0o000, 0o200, 0o000, 0x0580, 0o000, 0x03a0, 0o000, 0x05a0, 0o000, 0x03e0, 0o000, 0o200, 0o000, 0o200, 0o000, 0x05c0,
	0o000, 0x05c0, 0o000, 0x04c0, 0o000, 0x05e0, 0o000, 0x0600, 0o000, 0x0620, 0o000, 0x0640, 0o000, 0x0660, 0o000, 0o200,
	0o000, 0o200, 0o000, 0o200, 0o000, 0x01a0, 0o000, 0x0680, 0o000, 0x06a0, 0o000, 0x06c0, 0o000, 0x06e0, 0o000, 0x0700,
	0o000, 0x0720, 0o000, 0x0740, 0o000, 0x0760, 0o000, 0x0780, 0o000, 0x07a0, 0o000, 0x07c0, 0o000, 0x07e0, 0o000, 0x0800,
	0o000, 0x0820, 0o000, 0x0840, 0o000, 0x0860, 0o000, 0o200, 0o000, 0x0880, 0o000, 0o200, 0o000, 0x06e0, 0o000, 0x0720,
}

func zzUnpackRowMapNoParams() []int {
	result := make([]int, 104)
	offset := 0
	offset = zzUnpackRowMap(ZZ_ROWMAP_PACKED_0, offset, result)
	return result
}

func zzUnpackRowMap(packed []rune, offset int, result []int) int {
	i := 0           /* index in packed string  */
	j := offset      /* index in unpacked array */
	l := len(packed) // 208
	for i < l {
		high := packed[i] << 16
		i++
		result[j] = int(high | packed[i])
		i++
		j++
	}
	return j
}

/**
* The transition table of the DFA
 */
var ZZ_TRANS []int = zzUnpackTransNoParams()

var ZZ_TRANS_PACKED_0 []rune = []rune{
	0o001, 0o012, 0o001, 0o013, 0o001, 0o014, 0o001, 0o015, 0o003, 0o016, 0o001, 0o017, 0o001, 0o020, 0o001, 0o021,
	0o001, 0o022, 0o001, 0o023, 0o001, 0o024, 0o001, 0o016, 0o001, 0o025, 0o001, 0o016, 0o001, 0o026, 0o002, 0o022,
	0o001, 0o016, 0o001, 0o022, 0o002, 0o027, 0o001, 0o030, 0o001, 0o000, 0o001, 0o031, 0o002, 0o022, 0o001, 0o032,
	0o003, 0o022, 0o003, 0o033, 0o001, 0o034, 0o001, 0o035, 0o033, 0o033, 0o001, 0o036, 0o001, 0o037, 0o036, 0o036,
	0o002, 0o040, 0o001, 0o041, 0o035, 0o040, 0o040, 0o000, 0o001, 0o042, 0o001, 0o043, 0o036, 0o042, 0o001, 0o044,
	0o001, 0o045, 0o036, 0o044, 0o006, 0o046, 0o001, 0o047, 0o031, 0o046, 0o001, 0o050, 0o001, 0o051, 0o004, 0o050,
	0o001, 0o052, 0o031, 0o050, 0o003, 0o000, 0o001, 0o053, 0o001, 0o054, 0o034, 0o000, 0o001, 0o055, 0o005, 0o000,
	0o005, 0o022, 0o004, 0o000, 0o003, 0o022, 0o001, 0o000, 0o001, 0o022, 0o004, 0o000, 0o007, 0o022, 0o001, 0o000,
	0o001, 0o056, 0o005, 0o000, 0o005, 0o022, 0o004, 0o000, 0o003, 0o022, 0o001, 0o000, 0o001, 0o022, 0o004, 0o000,
	0o007, 0o022, 0o001, 0o000, 0o001, 0o057, 0o005, 0o000, 0o005, 0o022, 0o004, 0o000, 0o003, 0o022, 0o001, 0o000,
	0o001, 0o022, 0o004, 0o000, 0o007, 0o022, 0o007, 0o000, 0o005, 0o022, 0o004, 0o000, 0o003, 0o022, 0o001, 0o000,
	0o001, 0o022, 0o004, 0o000, 0o007, 0o022, 0o013, 0o000, 0o001, 0o023, 0o002, 0o000, 0o001, 0o060, 0o001, 0o000,
	0o001, 0o023, 0o001, 0o000, 0o001, 0o061, 0o001, 0o000, 0o001, 0o062, 0o030, 0o000, 0o001, 0o063, 0o026, 0o000,
	0o001, 0o064, 0o006, 0o000, 0o001, 0o065, 0o002, 0o000, 0o001, 0o066, 0o001, 0o000, 0o001, 0o065, 0o030, 0o000,
	0o001, 0o067, 0o001, 0o000, 0o001, 0o023, 0o002, 0o000, 0o001, 0o060, 0o001, 0o000, 0o001, 0o023, 0o001, 0o000,
	0o001, 0o061, 0o001, 0o000, 0o001, 0o062, 0o042, 0o000, 0o001, 0o053, 0o017, 0o000, 0o005, 0o022, 0o004, 0o000,
	0o003, 0o022, 0o001, 0o000, 0o001, 0o022, 0o004, 0o000, 0o001, 0o022, 0o001, 0o070, 0o003, 0o022, 0o001, 0o071,
	0o001, 0o022, 0o007, 0o000, 0o005, 0o022, 0o004, 0o000, 0o003, 0o022, 0o001, 0o000, 0o001, 0o022, 0o004, 0o000,
	0o004, 0o022, 0o001, 0o072, 0o002, 0o022, 0o003, 0o033, 0o002, 0o000, 0o033, 0o033, 0o004, 0o000, 0o001, 0o073,
	0o036, 0o000, 0o001, 0o074, 0o001, 0o075, 0o033, 0o000, 0o001, 0o036, 0o001, 0o000, 0o036, 0o036, 0o001, 0o000,
	0o001, 0o076, 0o023, 0o000, 0o001, 0o077, 0o001, 0o100, 0o011, 0o000, 0o002, 0o040, 0o001, 0o000, 0o035, 0o040,
	0o002, 0o000, 0o001, 0o101, 0o035, 0o000, 0o001, 0o042, 0o001, 0o000, 0o036, 0o042, 0o025, 0o000, 0o001, 0o102,
	0o001, 0o103, 0o011, 0o000, 0o001, 0o044, 0o001, 0o000, 0o036, 0o044, 0o025, 0o000, 0o001, 0o104, 0o001, 0o105,
	0o011, 0o000, 0o006, 0o046, 0o001, 0o000, 0o031, 0o046, 0o025, 0o053, 0o001, 0o000, 0o012, 0o053, 0o005, 0o000,
	0o001, 0o106, 0o045, 0o000, 0o001, 0o065, 0o002, 0o000, 0o001, 0o107, 0o001, 0o000, 0o001, 0o065, 0o001, 0o000,
	0o001, 0o061, 0o001, 0o000, 0o001, 0o062, 0o026, 0o000, 0o001, 0o110, 0o004, 0o000, 0o001, 0o110, 0o002, 0o000,
	0o001, 0o111, 0o003, 0o000, 0o001, 0o111, 0o023, 0o000, 0o001, 0o065, 0o004, 0o000, 0o001, 0o065, 0o001, 0o000,
	0o001, 0o061, 0o001, 0o000, 0o001, 0o062, 0o023, 0o000, 0o001, 0o112, 0o002, 0o000, 0o001, 0o112, 0o004, 0o000,
	0o003, 0o112, 0o001, 0o000, 0o001, 0o112, 0o022, 0o000, 0o005, 0o022, 0o004, 0o000, 0o003, 0o022, 0o001, 0o000,
	0o001, 0o022, 0o004, 0o000, 0o002, 0o022, 0o001, 0o113, 0o004, 0o022, 0o007, 0o000, 0o005, 0o022, 0o004, 0o000,
	0o003, 0o022, 0o001, 0o000, 0o001, 0o022, 0o004, 0o000, 0o006, 0o022, 0o001, 0o114, 0o003, 0o000, 0o001, 0o115,
	0o003, 0o000, 0o005, 0o022, 0o004, 0o000, 0o003, 0o022, 0o001, 0o000, 0o001, 0o022, 0o002, 0o116, 0o001, 0o117,
	0o001, 0o000, 0o007, 0o022, 0o001, 0o000, 0o001, 0o120, 0o023, 0o000, 0o002, 0o077, 0o036, 0o000, 0o001, 0o077,
	0o001, 0o100, 0o012, 0o000, 0o001, 0o121, 0o023, 0o000, 0o002, 0o102, 0o012, 0o000, 0o001, 0o122, 0o023, 0o000,
	0o002, 0o104, 0o024, 0o000, 0o001, 0o110, 0o004, 0o000, 0o001, 0o110, 0o026, 0o000, 0o005, 0o022, 0o004, 0o000,
	0o003, 0o022, 0o001, 0o000, 0o001, 0o022, 0o004, 0o000, 0o002, 0o022, 0o001, 0o123, 0o004, 0o022, 0o003, 0o000,
	0o001, 0o124, 0o003, 0o000, 0o005, 0o022, 0o004, 0o000, 0o003, 0o022, 0o001, 0o000, 0o001, 0o022, 0o002, 0o125,
	0o001, 0o126, 0o001, 0o000, 0o007, 0o022, 0o003, 0o000, 0o001, 0o127, 0o037, 0o000, 0o001, 0o115, 0o021, 0o000,
	0o002, 0o116, 0o001, 0o117, 0o001, 0o000, 0o001, 0o130, 0o035, 0o000, 0o001, 0o127, 0o013, 0o000, 0o001, 0o131,
	0o037, 0o000, 0o001, 0o124, 0o021, 0o000, 0o002, 0o125, 0o001, 0o126, 0o001, 0o000, 0o001, 0o132, 0o035, 0o000,
	0o001, 0o131, 0o010, 0o000, 0o025, 0o127, 0o001, 0o116, 0o003, 0o127, 0o001, 0o133, 0o006, 0o127, 0o032, 0o000,
	0o001, 0o134, 0o005, 0o000, 0o025, 0o131, 0o001, 0o125, 0o003, 0o131, 0o001, 0o135, 0o006, 0o131, 0o032, 0o000,
	0o001, 0o136, 0o005, 0o000, 0o025, 0o127, 0o001, 0o116, 0o003, 0o127, 0o001, 0o133, 0o001, 0o137, 0o005, 0o127,
	0o033, 0o000, 0o001, 0o140, 0o004, 0o000, 0o025, 0o131, 0o001, 0o125, 0o003, 0o131, 0o001, 0o135, 0o001, 0o141,
	0o005, 0o131, 0o033, 0o000, 0o001, 0o142, 0o004, 0o000, 0o025, 0o127, 0o001, 0o116, 0o003, 0o127, 0o001, 0o133,
	0o001, 0o127, 0o001, 0o143, 0o004, 0o127, 0o033, 0o000, 0o001, 0o144, 0o004, 0o000, 0o025, 0o131, 0o001, 0o125,
	0o003, 0o131, 0o001, 0o135, 0o001, 0o131, 0o001, 0o145, 0o004, 0o131, 0o033, 0o000, 0o001, 0o146, 0o004, 0o000,
	0o025, 0o127, 0o001, 0o116, 0o003, 0o127, 0o001, 0o133, 0o001, 0o127, 0o001, 0o147, 0o004, 0o127, 0o025, 0o131,
	0o001, 0o125, 0o003, 0o131, 0o001, 0o135, 0o001, 0o131, 0o001, 0o150, 0o004, 0o131,
}

func zzUnpackTransNoParams() []int {
	result := make([]int, 2208)
	offset := 0
	offset = zzUnpackTrans(ZZ_TRANS_PACKED_0, offset, result)
	return result
}

func zzUnpackTrans(packed []rune, offset int, result []int) int {
	i := 0           /* index in packed string  */
	j := offset      /* index in unpacked array */
	l := len(packed) // 780
	for i < l {
		count := packed[i]
		i++
		value := packed[i]
		i++
		value--
		result[j] = int(value)
		j++
		count--
		for count > 0 {
			result[j] = int(value)
			j++
			count--
		}
	}
	return j
}

/* error codes */
const (
	ZZ_UNKNOWN_ERROR = 0
	ZZ_NO_MATCH      = 1
	ZZ_PUSHBACK_2BIG = 2
)

/* error messages for the codes above */
var ZZ_ERROR_MSG []string = []string{
	"Unknown internal scanner error",
	"Error: could not match input",
	"Error: pushback Value was too large",
}

/**
* ZZ_ATTRIBUTE[aState] contains the attributes of state <code>aState</code>
 */
var ZZ_ATTRIBUTE []int = zzUnpackAttributeNoParams()

var ZZ_ATTRIBUTE_PACKED_0 []rune = []rune{
	0o004, 0o000, 0o001, 0o010, 0o004, 0o000, 0o003, 0o011, 0o001, 0o001, 0o001, 0o011, 0o010, 0o001, 0o001, 0o011,
	0o017, 0o001, 0o004, 0o011, 0o001, 0o001, 0o001, 0o011, 0o001, 0o000, 0o002, 0o011, 0o001, 0o001, 0o001, 0o000,
	0o003, 0o011, 0o001, 0o001, 0o001, 0o011, 0o001, 0o000, 0o003, 0o001, 0o002, 0o011, 0o001, 0o000, 0o001, 0o011,
	0o002, 0o000, 0o001, 0o011, 0o004, 0o000, 0o002, 0o011, 0o001, 0o001, 0o001, 0o000, 0o003, 0o001, 0o003, 0o000,
	0o003, 0o011, 0o001, 0o001, 0o020, 0o000, 0o001, 0o011, 0o001, 0o000, 0o001, 0o011, 0o002, 0o001,
}

func zzUnpackAttributeNoParams() []int {
	result := make([]int, 104)
	offset := 0
	offset = zzUnpackAttribute(ZZ_ATTRIBUTE_PACKED_0, offset, result)
	return result
}

func zzUnpackAttribute(packed []rune, offset int, result []int) int {
	i := 0           /* index in packed string  */
	j := offset      /* index in unpacked array */
	l := len(packed) // 78
	for i < l {
		count := packed[i]
		i++
		value := packed[i]
		i++
		result[j] = int(value)
		j++
		count--

		for count > 0 {
			result[j] = int(value)
			j++
			count--
		}
	}
	return j
}

type Lexer struct {
	/** the input device */
	zzReader io.RuneReader

	/** the current state of the DFA */
	zzState int

	/** the current lexical state */
	zzLexicalState int

	/** this buffer contains the current text to be matched and is
	the source of the yytext() string */
	zzBuffer []rune

	// zzBytesBuffer []byte

	/** the textposition at the last accepting state */
	zzMarkedPos int

	/** the current text Position in the buffer */
	zzCurrentPos int

	/** startRead marks the beginning of the yytext() string in the buffer */
	zzStartRead int

	/** endRead marks the last character in the buffer, that has been read
	from input */
	zzEndRead int

	/** number of newlines encountered up to the start of the matched text */
	yyline int

	/** the number of characters up to the start of the matched text */
	yychar int

	/**
	* the number of characters from the last newline up to the start of the
	* matched text
	 */
	yycolumn int

	/**
	* zzAtBOL == true <=> the scanner is currently at the beginning of a line
	 */
	zzAtBOL bool

	/** zzAtEOF == true <=> the scanner is at the EOF */
	zzAtEOF bool

	/** denotes if the user-EOF-code has already been executed */
	zzEOFDone bool

	/**
	* The number of occupied positions in zzBuffer beyond zzEndRead.
	* When a lead/high surrogate has been read from the input stream
	* into the final zzBuffer Position, this will have a Value of 1;
	* otherwise, it will have a Value of 0.
	 */
	zzFinalHighSurrogate int

	/* user code: */
	ltstr     string
	debugFlag bool
}

func (lexer *Lexer) init() {
	lexer.zzLexicalState = YYINITIAL
	lexer.zzBuffer = make([]rune, ZZ_BUFFERSIZE)
	lexer.zzAtBOL = true
}

func (lexer *Lexer) Reset(in io.RuneReader) *Lexer {
	lexer.zzLexicalState = YYINITIAL
	lexer.zzAtBOL = true
	lexer.zzReader = in
	lexer.zzState = 0
	lexer.zzMarkedPos = 0
	lexer.zzCurrentPos = 0
	lexer.zzStartRead = 0
	lexer.zzEndRead = 0
	lexer.yyline = 0
	lexer.yychar = 0
	lexer.yycolumn = 0
	lexer.zzAtEOF = false
	lexer.zzEOFDone = false
	lexer.zzFinalHighSurrogate = 0
	lexer.ltstr = ""
	return lexer
}

func (lexer *Lexer) debug(info string) {
	if !lexer.debugFlag {
		return
	}
}

func (lexer *Lexer) yyerror(msg string) {
	locInfo := "(line: " + strconv.Itoa(lexer.yyline) + ", column: " + strconv.Itoa(lexer.yycolumn) + ", char: " + strconv.Itoa(lexer.yychar) + ")"
	if msg == "" {
		panic("syntex error" + locInfo)
	} else {
		panic("syntex error" + locInfo + ": " + msg)
	}
}

/**
* Creates a new scanner
*
* @param   in  the java.io.Reader to read input from.
 */
func NewLexer(in io.RuneReader, debug bool) *Lexer {
	l := new(Lexer)
	l.init()
	l.debugFlag = debug
	l.zzReader = in
	return l
}

/**
* Unpacks the compressed character translation table.
*
* @param packed   the packed character translation table
* @return         the unpacked character translation table
 */
func zzUnpackCMap(packed []rune) []rune {
	m := make([]rune, 0x110000)
	i := 0 /* index in packed string  */
	j := 0 /* index in unpacked array */
	for i < 208 {
		count := packed[i]
		i++
		value := packed[i]
		i++
		m[j] = value
		j++
		count--
		for count > 0 {
			m[j] = value
			j++
			count--
		}
	}
	return m
}

/**
* Refills the input buffer.
*
* @return      <code>false</code>, iff there was new input.
*
* @exception   java.io.IOException  if any I/O-Error occurs
 */
func (lexer *Lexer) zzRefill() (bool, error) {
	/* first: make room (if you can) */
	if lexer.zzStartRead > 0 {
		lexer.zzEndRead += lexer.zzFinalHighSurrogate
		lexer.zzFinalHighSurrogate = 0
		l := lexer.zzEndRead - lexer.zzStartRead
		if l > 0 {
			copy(lexer.zzBuffer[:l], lexer.zzBuffer[lexer.zzStartRead:lexer.zzEndRead])
		}

		/* translate stored positions */
		lexer.zzEndRead -= lexer.zzStartRead
		lexer.zzCurrentPos -= lexer.zzStartRead
		lexer.zzMarkedPos -= lexer.zzStartRead
		lexer.zzStartRead = 0
	}

	/* is the buffer big enough? */
	if lexer.zzCurrentPos >= len(lexer.zzBuffer)-lexer.zzFinalHighSurrogate {
		/* if not: blow it up */
		newBuffer := make([]rune, len(lexer.zzBuffer)*2)

		copy(newBuffer[:len(lexer.zzBuffer)], lexer.zzBuffer[:len(lexer.zzBuffer)])
		lexer.zzBuffer = newBuffer
		lexer.zzEndRead += lexer.zzFinalHighSurrogate
		lexer.zzFinalHighSurrogate = 0
	}

	/* fill the buffer with new input */
	requested := len(lexer.zzBuffer) - lexer.zzEndRead

	numRead := 0
	for i := lexer.zzEndRead; i < lexer.zzEndRead+requested; i++ {
		r, _, err := lexer.zzReader.ReadRune()
		if err == io.EOF {
			if numRead == 0 {
				numRead = -1
			}
			break
		} else if err != nil {
			return false, err
		} else {
			numRead++
			lexer.zzBuffer[i] = r
		}
	}

	/* not supposed to occur according to specification of java.io.Reader */
	if numRead == 0 {
		panic("Reader returned 0 characters. See JFlex examples for workaround.")
	}

	if numRead > 0 {

		lexer.zzEndRead += numRead
		/* If numRead == requested, we might have requested to few chars to
		   encode a full Unicode character. We assume that a Reader would
		   otherwise never return half characters. */
		if numRead == requested {
			if utf8.ValidRune(lexer.zzBuffer[lexer.zzEndRead-1]) {
				lexer.zzEndRead--
				lexer.zzFinalHighSurrogate = 1
			}
		}
		/* potentially more input available */
		return false, nil
	}

	/* numRead < 0 ==> end of stream */
	return true, nil
}

/**
* Closes the input stream.
 */
func (lexer *Lexer) yyclose() error {
	lexer.zzAtEOF = true                /* indicate end of file */
	lexer.zzEndRead = lexer.zzStartRead /* invalidate buffer    */

	if lexer.zzReader != nil {
		if c, ok := lexer.zzReader.(io.Closer); ok {
			return c.Close()
		}
	}
	return nil
}

/**
* Resets the scanner to read from a new input stream.
* Does not close the old reader.
*
* All internal variables are reset, the old input stream
* <b>cannot</b> be reused (internal buffer is discarded and lost).
* Lexical state is set to <tt>ZZ_INITIAL</tt>.
*
* Internal scan buffer is resized down to its initial length, if it has grown.
*
* @param reader   the new input stream
 */
func (lexer *Lexer) yyreset(reader io.RuneReader) {
	lexer.zzReader = reader
	lexer.zzAtBOL = true
	lexer.zzAtEOF = false
	lexer.zzEOFDone = false
	lexer.zzEndRead = 0
	lexer.zzStartRead = 0
	lexer.zzCurrentPos = 0
	lexer.zzMarkedPos = 0
	lexer.zzFinalHighSurrogate = 0
	lexer.yyline = 0
	lexer.yychar = 0
	lexer.yycolumn = 0
	lexer.zzLexicalState = YYINITIAL
	if len(lexer.zzBuffer) > ZZ_BUFFERSIZE {
		lexer.zzBuffer = make([]rune, ZZ_BUFFERSIZE)
	}
}

/**
* Returns the current lexical state.
 */
func (lexer *Lexer) yystate() int {
	return lexer.zzLexicalState
}

/**
* Enters a new lexical state
*
* @param newState the new lexical state
 */
func (lexer *Lexer) yybegin(newState int) {
	lexer.zzLexicalState = newState
}

/**
* Returns the text matched by the current regular expression.
 */
func (lexer *Lexer) yytext() string {
	return string(lexer.zzBuffer[lexer.zzStartRead:lexer.zzMarkedPos])
}

/**
* Returns the character at Position <tt>pos</tt> from the
* matched text.
*
* It is equivalent to yytext().charAt(pos), but faster
*
* @param pos the Position of the character to fetch.
*            A Value from 0 to yylength()-1.
*
* @return the character at Position pos
 */
func (lexer *Lexer) yycharat(pos int) rune {
	return lexer.zzBuffer[lexer.zzStartRead+pos]
}

/**
* Returns the length of the matched text region.
 */
func (lexer *Lexer) yylength() int {
	return lexer.zzMarkedPos - lexer.zzStartRead
}

/**
* Reports an error that occured while scanning.
*
* In a wellformed scanner (no or only correct usage of
* yypushback(int) and a match-all fallback rule) this method
* will only be called with things that "Can't Possibly Happen".
* If this method is called, something is seriously wrong
* (e.g. a JFlex bug producing a faulty scanner etc.).
*
* Usual syntax/scanner level error handling should be done
* in error fallback rules.
*
* @param   errorCode  the code of the errormessage to display
 */
func (lexer *Lexer) zzScanError(errorCode int) {
	var message string

	message = ZZ_ERROR_MSG[errorCode]
	if message == "" {
		message = ZZ_ERROR_MSG[ZZ_UNKNOWN_ERROR]
	}

	panic(message)
}

/**
* Pushes the specified amount of characters back into the input stream.
*
* They will be read again by then next call of the scanning method
*
* @param number  the number of characters to be read again.
*                This number must not be greater than yylength()!
 */
func (lexer *Lexer) yypushback(number int) {
	if number > lexer.yylength() {
		lexer.zzScanError(ZZ_PUSHBACK_2BIG)
	}

	lexer.zzMarkedPos -= number
}

/**
* Resumes scanning until the next regular expression is matched,
* the end of input is encountered or an I/O-Error occurs.
*
* @return      the next token
* @exception   java.io.IOException  if any I/O-Error occurs
 */
func (lexer *Lexer) Yylex() (*LVal, error) {
	var zzInput rune
	var zzAction, zzCurrentPosL, zzMarkedPosL int
	// cached fields:
	zzEndReadL := lexer.zzEndRead
	zzBufferL := lexer.zzBuffer
	zzCMapL := ZZ_CMAP

	zzTransL := ZZ_TRANS
	zzRowMapL := ZZ_ROWMAP
	zzAttrL := ZZ_ATTRIBUTE

	for {
		zzMarkedPosL = lexer.zzMarkedPos

		lexer.yychar += zzMarkedPosL - lexer.zzStartRead

		zzR := false
		var zzCh rune
		var zzCharCount int
		zzCurrentPosL = lexer.zzStartRead
		for zzCurrentPosL < zzMarkedPosL {
			zzCh = zzBufferL[zzCurrentPosL]
			zzCharCount = utf8.RuneLen(zzCh)
			switch zzCh {
			case '\u000B', '\u000C', '\u0085', '\u2028', '\u2029':
				lexer.yyline++
				lexer.yycolumn = 0
				zzR = false
			case '\r':
				lexer.yyline++
				lexer.yycolumn = 0
				zzR = true
			case '\n':
				if zzR {
					zzR = false
				} else {
					lexer.yyline++
					lexer.yycolumn = 0
				}
			default:
				zzR = false
				lexer.yycolumn += zzCharCount
			}
			zzCurrentPosL += zzCharCount
		}

		if zzR {
			// peek one character ahead if it is \n (if we have counted one line too much)
			var zzPeek bool
			if zzMarkedPosL < zzEndReadL {
				zzPeek = zzBufferL[zzMarkedPosL] == '\n'
			} else if lexer.zzAtEOF {
				zzPeek = false
			} else {
				eof, err := lexer.zzRefill()
				if err != nil {
					return nil, err
				}
				zzEndReadL = lexer.zzEndRead
				zzMarkedPosL = lexer.zzMarkedPos
				zzBufferL = lexer.zzBuffer
				if eof {
					zzPeek = false
				} else {
					zzPeek = zzBufferL[zzMarkedPosL] == '\n'
				}

			}
			if zzPeek {
				lexer.yyline--
			}
		}
		zzAction = -1

		zzCurrentPosL = zzMarkedPosL
		lexer.zzCurrentPos = zzMarkedPosL
		lexer.zzStartRead = zzMarkedPosL
		lexer.zzState = ZZ_LEXSTATE[lexer.zzLexicalState]

		// set up zzAction for empty match case:
		zzAttributes := zzAttrL[lexer.zzState]
		if (zzAttributes & 1) == 1 {
			zzAction = lexer.zzState
		}

		{
			for true {

				if zzCurrentPosL < zzEndReadL {
					zzInput = zzBufferL[zzCurrentPosL]
					zzCurrentPosL += 1 // utf8.RuneLen(zzInput)
				} else if lexer.zzAtEOF {
					zzInput = YYEOF
					goto zzForAction
				} else {
					// store back cached positions
					lexer.zzCurrentPos = zzCurrentPosL
					lexer.zzMarkedPos = zzMarkedPosL
					eof, err := lexer.zzRefill()
					if err != nil {
						return nil, err
					}
					// get translated positions and possibly new buffer
					zzCurrentPosL = lexer.zzCurrentPos
					zzMarkedPosL = lexer.zzMarkedPos
					zzBufferL = lexer.zzBuffer
					zzEndReadL = lexer.zzEndRead
					if eof {
						zzInput = YYEOF
						goto zzForAction
					} else {
						zzInput = zzBufferL[zzCurrentPosL]
						zzCurrentPosL += 1 // utf8.RuneLen(zzInput)
					}
				}

				zzNext := zzTransL[zzRowMapL[lexer.zzState]+int(zzCMapL[zzInput])]
				if zzNext == -1 {
					goto zzForAction
				}

				lexer.zzState = zzNext

				zzAttributes = zzAttrL[lexer.zzState]
				if (zzAttributes & 1) == 1 {
					zzAction = lexer.zzState
					zzMarkedPosL = zzCurrentPosL
					if (zzAttributes & 8) == 8 {
						goto zzForAction
					}
				}

			}
		}

	zzForAction:
		// store back cached Position
		lexer.zzMarkedPos = zzMarkedPosL

		if zzInput == YYEOF && lexer.zzStartRead == lexer.zzCurrentPos {
			lexer.zzAtEOF = true
			switch lexer.zzLexicalState {
			case xc:
				{
					lexer.debug("<xc><<EOF>>")

					lexer.yybegin(YYINITIAL)
					lexer.yyerror("unterminated /* comment")
				}
			case 105:
			case xq:
				{
					lexer.debug("<xq><<EOF>>")

					lexer.yybegin(YYINITIAL)
					lexer.yyerror("unterminated quoted string")
				}
				fallthrough
			case 106:
			case xdq:
				{
					lexer.debug("<xdq><<EOF>>")

					lexer.yybegin(YYINITIAL)
					lexer.yyerror("unterminated quoted identifier")
				}
				fallthrough
			case 107:
			case xbin:
				{
					lexer.debug("<xbin><<EOF>>")

					lexer.yybegin(YYINITIAL)
					lexer.yyerror("unterminated binary string literal")
				}
				fallthrough
			case 108:
			case xhex:
				{
					lexer.debug("<xhex><<EOF>>")

					lexer.yybegin(YYINITIAL)
					lexer.yyerror("unterminated hexadecimal integer")
				}
				fallthrough
			case 109:
			case xq2:
				{
					lexer.yybegin(YYINITIAL)
					lexer.yyerror("unterminated q2 string")
				}
				fallthrough
			case 110:
			case xq2_2:
				{
					lexer.yybegin(YYINITIAL)
					lexer.yyerror("unterminated q2 string")
				}
				fallthrough
			case 111:
			default:
				return nil, nil
			}
		} else {
			var action int
			if zzAction < 0 {
				action = zzAction
			} else {
				action = ZZ_ACTION[zzAction]
			}
			switch action {
			case 1:
				{
					lexer.debug("{other}")

					return newLVal(lexer.yytext(), NORMAL), nil
				}
				fallthrough
			case 44:
			case 2:
				{
					lexer.debug("{xq_start}")

					lexer.yybegin(xq)
					lexer.ltstr = ""
				}
				fallthrough
			case 45:
			case 3:
				{
					lexer.debug("{xdq_start}")

					lexer.yybegin(xdq)
					lexer.ltstr = ""
					lexer.ltstr += lexer.yytext()
				}
				fallthrough
			case 46:
			case 4:
				{
					lexer.debug("{self} | {op_chars}")

					return newLVal(lexer.yytext(), NORMAL), nil
				}
				fallthrough
			case 47:
			case 5:
				{
					lexer.debug("{identifier}")

					return newLVal(lexer.yytext(), NORMAL), nil
				}
				fallthrough
			case 48:
			case 6:
				{
					lexer.debug("{integer}")

					return newLVal(lexer.yytext(), INT), nil
				}
				fallthrough
			case 49:
			case 7:
				{
					lexer.debug("{whitespace} | {comment} | {c_line_comment}")

					return newLVal(lexer.yytext(), WHITESPACE_OR_COMMENT), nil
				}
				fallthrough
			case 50:
			case 8:
				{
					lexer.debug("<xc>{xc_inside}")

					lexer.ltstr += lexer.yytext()
				}
				fallthrough
			case 51:
			case 9:
				{
					lexer.debug("<xc>[\\/] | <xc>[\\*]")

					lexer.ltstr += lexer.yytext()
				}
				fallthrough
			case 52:
			case 10:
				{
					lexer.debug("<xq>{xq_inside}")

					lexer.ltstr += lexer.yytext()
				}
				fallthrough
			case 53:
			case 11:
				{
					lexer.debug("<xq>{xq_stop}")

					lexer.yybegin(YYINITIAL)
					return newLVal(lexer.ltstr, STRING), nil
				}
				fallthrough
			case 54:
			case 12:
				{
					lexer.debug("<xdq>{xdq_inside}")

					lexer.ltstr += lexer.yytext()
				}
				fallthrough
			case 55:
			case 13:
				{
					lexer.debug("<xdq>{xdq_stop}")

					lexer.yybegin(YYINITIAL)
					lexer.ltstr += lexer.yytext()
					return newLVal(lexer.ltstr, NORMAL), nil
				}
				fallthrough
			case 56:
			case 14:
				{
					lexer.debug("<xbin>{xbin_inside}")

					lexer.ltstr += lexer.yytext()
				}
				fallthrough
			case 57:
			case 15:
				{
					lexer.debug("<xbin>{xbin_stop}")

					lexer.yybegin(YYINITIAL)
					lexer.ltstr += lexer.yytext()
					return newLVal(lexer.ltstr, NORMAL), nil
				}
				fallthrough
			case 58:
			case 16:
				{
					lexer.debug("<xhex>{xhex_inside}")

					lexer.ltstr += lexer.yytext()
				}
				fallthrough
			case 59:
			case 17:
				{
					lexer.debug("<xhex>{xhex_stop}")

					lexer.yybegin(YYINITIAL)
					lexer.ltstr += lexer.yytext()
					return newLVal(lexer.ltstr, NORMAL), nil
				}
				fallthrough
			case 60:
			case 18:
				{
					lexer.ltstr += lexer.yytext()
				}
				fallthrough
			case 61:
			case 19:
				{
					lexer.yybegin(xq2_2)
				}
				fallthrough
			case 62:
			case 20:
				{
					lexer.ltstr += "]"
					lexer.ltstr += lexer.yytext()
					lexer.yybegin(xq2)
				}
				fallthrough
			case 63:
			case 21:
				{
					lexer.yybegin(YYINITIAL)

					return newLVal(lexer.ltstr, STRING), nil
				}
				fallthrough
			case 64:
			case 22:
				{
					lexer.ltstr += "]"
					lexer.yybegin(xq2_2)
				}
				fallthrough
			case 65:
			case 23:
				{
					lexer.debug("{xc_start}")

					lexer.yybegin(xc)
					lexer.ltstr = lexer.yytext()
				}
				fallthrough
			case 66:
			case 24:
				{
					lexer.debug("{xbin_start}")

					lexer.yybegin(xbin)
					lexer.ltstr = ""
					lexer.ltstr += lexer.yytext()
				}
				fallthrough
			case 67:
			case 25:
				{
					lexer.debug("{xhex_start}")

					lexer.yybegin(xhex)
					lexer.ltstr = ""
					lexer.ltstr += lexer.yytext()
				}
				fallthrough
			case 68:
			case 26:
				{
					lexer.debug("{decimal}")

					return newLVal(lexer.yytext(), DECIMAL), nil
				}
				fallthrough
			case 69:
			case 27:
				{
					lexer.debug("{real}")

					return newLVal(lexer.yytext(), DOUBLE), nil
				}
				fallthrough
			case 70:
			case 28:
				{
					lexer.debug("{assign}")

					return newLVal(lexer.yytext(), NORMAL), nil
				}
				fallthrough
			case 71:
			case 29:
				{
					lexer.debug("{selstar}")

					return newLVal(lexer.yytext(), NORMAL), nil
				}
				fallthrough
			case 72:
			case 30:
				{
					lexer.debug("{boundary}")

					return newLVal(lexer.yytext(), NORMAL), nil
				}
				fallthrough
			case 73:
			case 31:
				{
					lexer.debug("<xc>{xc_start}")

					lexer.ltstr += lexer.yytext()
				}
				fallthrough
			case 74:
			case 32:
				{
					lexer.debug("<xc>{xc_stop}")

					lexer.yybegin(YYINITIAL)
					lexer.ltstr += lexer.yytext()
					return newLVal(lexer.ltstr, WHITESPACE_OR_COMMENT), nil
				}
				fallthrough
			case 75:
			case 33:
				{
					lexer.debug("<xq>{xq_double}")

					lexer.ltstr += "\\'"
				}
				fallthrough
			case 76:
			case 34:
				{ // keep original string
					lexer.debug("<xdq>{xdq_double}")

					lexer.ltstr += lexer.yytext()
				}
				fallthrough
			case 77:
			case 35:
				{
					lexer.yybegin(xq2)
					lexer.ltstr = ""
				}
				fallthrough
			case 78:
			case 36:
				{
					lexer.debug("{integer_with_boundary}")

					return newLVal(lexer.yytext(), NORMAL), nil
				}
				fallthrough
			case 79:
			case 37:
				{
					lexer.debug("{hex_integer}")

					return newLVal(lexer.yytext(), HEX_INT), nil
				}
				fallthrough
			case 80:
			case 38:
				{
					lexer.debug("<xq>{xq_cat}")
				}
				fallthrough
			case 81:
			case 39:
				{ /* ignore */
					lexer.debug("<xbin>{xbin_cat}")
				}
				fallthrough
			case 82:
			case 40:
				{
					lexer.debug("<xhex>{xhex_cat}")
				}
				fallthrough
			case 83:
			case 41:
				{
					lexer.debug("{null}")

					return newLVal("null", NULL), nil
				}
				fallthrough
			case 84:
			case 42:
				{
					lexer.debug("{is_null}")

					return newLVal(lexer.yytext(), NORMAL), nil
				}
				fallthrough
			case 85:
			case 43:
				{
					lexer.debug("{not_null}")

					return newLVal(lexer.yytext(), NORMAL), nil
				}
				fallthrough
			case 86:
			default:
				lexer.zzScanError(ZZ_NO_MATCH)
			}
		}
	}
}
