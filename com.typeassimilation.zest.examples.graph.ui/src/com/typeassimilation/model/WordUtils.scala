package com.typeassimilation.model

object WordUtils {
  def lowerCaseFirstLetter(s: String) = if (s.isEmpty) s else s.charAt(0).toLower.toString + s.substring(1)
  def upperCaseFirstLetter(s: String) = if (s.isEmpty) s else s.charAt(0).toUpper.toString + s.substring(1)
  def removeTrailingFullStop(s: String) = if (!s.isEmpty && s.last == '.') s.substring(0, s.length - 1) else s
  def removeUnecessaryWhitespace(s: String) = s.trim.replaceAll("\\s{2,}", " ")
  def removeDoubleThe(s: String) = s.replaceAllLiterally("the the", "the")
  def prepareForAppending(s: String) = removeLeadingThe(removeTrailingFullStop(lowerCaseFirstLetter(removeUnecessaryWhitespace(s))))
  def removeLeadingThe(s: String) = if (s.startsWith("the ")) s.substring(4).trim else s
  def sentenceCaseAndTerminate(s: String) = upperCaseFirstLetter(removeDoubleThe(removeUnecessaryWhitespace(s))) + "."
  def isVowel(c: Char) = Set('a', 'e', 'i', 'o', 'u').contains(c.toLower)
  def anOrA(followingWord: String) = if (!followingWord.isEmpty && isVowel(followingWord.charAt(0))) "an" else "a"
  def rankToWord(rank: Int) = rank match {
    case 1 => "first"
    case 2 => "second"
    case 3 => "third"
    case 4 => "fourth"
    case 5 => "fifth"
    case 6 => "sixth"
    case 7 => "seventh"
    case 8 => "eighth"
    case 9 => "ninth"
    case 10 => "tenth"
    case 11 => "eleventh"
    case 12 => "twelth"
  }
}