class Solution(object):
    def strStr(self, haystack, needle):
        len_h = len(haystack)
        len_n = len(needle)
        i=0
        
        for i in range(len(haystack) - len(needle) + 1):
            j = 0
            if haystack[i] == needle[0]:
                while j<len_n and i+j<(len_h):
                    if haystack[i+j] == needle[j]: j+=1
                    else:break
            if j == len_n: return i
        return -1


    


if __name__ == '__main__':
    solution = Solution()
    print(solution.strStr(haystack="hello", needle = "ll"))
     
        