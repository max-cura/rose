.globl _ets_lup_sli

//return (log2 = 63 - __builtin_clzl (size)) ?
//    (2 * log2)
//      - !(~(1 << (log2)) & size)
//      + ((1 << (log2 - 1)) & size && ~(3 << (log2 - 1)) & size)
//    : 0;

// !(~(1 << log2)) & size)
//  ~(1 << log2) = reverse mask
//  ~(1 << log2) & size != 0 iff popcnt size > 1
// !(~(1 << log2) & size) != 0 iff popcnt size == 1
// If popcnt is one, then `decl %eax` and `retq`

// (1 << (log2 - 1)) & size -> true if secondary bit is set
// ~(3 << (log2 - 1)) & size -> true if secondary bit is set & popcnt > 2

// scratch: RDI (arg), RSI, RDX, RCX, R8, R9
_ets_lup_sli:
    movq %rdi, %r8
    // = number of 0s to the right of the most significant 1
    bsrq %rdi, %rax
    movq %rax, %rdx

    // if size <= 1, return 0
#if !BRANCHLESS
#    cmpq $16, %rdi
#    jle ___nvh_aslslup.q_r0
#endif

    // 2 * log2 -> rax
    shl $1, %rax

    // if only one bit is set...
    btrq %rdx, %rdi // (~(1 << log2) & size)
# if BRANCHLESS
    xorq %rsi
    testq %rdi, %rdi
    setnz %sil
#else
    # jz ___nvh_aslslup.q_popcnt_1
#endif

    decq %rdx
    btrq %rdx, %rdi // Clear Bit(log2 - 1) on size
#if BRANCHLESS
    setnc %cl
    andb %cl, %sil
#else
    # jnc ___nvh_aslslup.q_ret // If Bit(log2 - 1) was 0, return 2 * log2
#endif
    testq %rdi, %rdi
#if BRANCHLESS
    setnz %cl
    andb %cl, %sil
    addq %rsi, %rax
#else
    # jz ___nvh_aslslup.q_ret // If popcnt(size) == 0, return 2 * log2
#endif

    // incq %rax
    subq $7, %rax
#if BRANCHLESS
    cmpq $16, %r8
    cmovleq $0, %rax
#endif
    retq
#if !BRANCHLESS
#___nvh_aslslup.q_popcnt_1:
#    subq $7, %rax
#    // decq %rax
#    ret
#___nvh_aslslup.q_r0:
#    movq $0, %rax
#___nvh_aslslup.q_ret:
#    retq
#endif
