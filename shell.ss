;; SPDX-License-Identifier: MIT

#!chezscheme
(library (shell)
  (export
   !
   $
   pipe
   shell:verbose
   ;; TODO add input / output redirection
   )
  (import
   (scheme)
   (swish imports))

  (define max-cmd-time (* 10 60 1000))

  (define shell:verbose (make-parameter #f))

  (define (verbose? level)
    (and (shell:verbose)
         (>= (shell:verbose) level)))

  (define-syntax LOG
    (syntax-rules ()
      [(_ level fmt arg ...)
       (when (verbose? level)
         (fprintf (console-error-port) fmt arg ...))]))

  (define (run-command quiet? $cmd $args)
    (define me self)
    (define cmd (format "~a" $cmd))
    (define args (map (lambda (x) (format "~a" x)) $args))
    (LOG 1 "~a~{ ~a~}\n" $cmd $args)
    (let-values ([(to-stdin from-stdout from-stderr os-pid) (spawn-os-process cmd args me)])
      (define stdout (binary->utf8 from-stdout))
      (define stderr (binary->utf8 from-stderr))
      (define (snag name ip)
        (lambda ()
          (let lp ([ls '()])
            (match (get-line ip)
              [#!eof
               (close-port ip)
               (send me `#(,name ,os-pid ,(reverse ls)))]
              [,line (lp (cons line ls))]))))
      (spawn&link (snag 'stdout stdout))
      (spawn&link (snag 'stderr stderr))
      (close-port to-stdin)
      (on-exit (begin (close-port stdout) (close-port stderr))
        (receive (after max-cmd-time (throw 'timeout))
          [#(process-terminated ,@os-pid ,exit-status ,signal)
           (unless (or quiet? (zero? exit-status))
             (LOG 1 "~a exited with status ~s [signal ~s]\n" cmd exit-status signal)
             (errorf cmd "~a"
               (receive (after 1000 throw 'no-stderr)
                 [#(stderr ,@os-pid ,lines) (join lines #\newline)])))
           (if quiet?
               exit-status
               (receive (after 1000 (throw 'no-stdout))
                 [#(stdout ,@os-pid ,ls) ls]))]))))

  (define-syntax $
    (syntax-rules ()
      [(_ -q e0 e1 ...)
       (eq? (datum -q) '-q)
       (run-command #t `e0 `(e1 ...))]
      [(_  e0 e1 ...)
       (run-command #f `e0 `(e1 ...))]))

  (define (tidy x)
    (parameterize ([print-extended-identifiers #t])
      (format "~a" x)))

  (define (make-pipeline-command check-exit-code? $cmd $args next-pid)
    (define me self)
    (define cmd (tidy $cmd))
    (define args (map tidy $args))
    (LOG 1 "~a~{ ~a~}\n" $cmd $args)
    (let-values ([(to-stdin from-stdout from-stderr os-pid) (spawn-os-process cmd args me)])
      (define stdin (binary->utf8 to-stdin))
      (define stdout (binary->utf8 from-stdout))
      (define stderr (binary->utf8 from-stderr))
      (define (forward to name ip)
        (lambda ()
          (let lp ()
            (let ([x (get-line ip)])
              (send to `#(,name ,os-pid ,x))
              (cond
               [(eof-object? x) (close-port ip)]
               [else (lp)])))))
      (spawn&link (forward next-pid 'stdout stdout))
      (spawn&link (forward me 'stderr stderr))
      (process-trap-exit #t)
      (let lp ([deadline (+ (erlang:now) max-cmd-time)])
        (receive
         (until (or deadline (+ (erlang:now) 200))
           (cond
            [(and (port-closed? stdin) (port-closed? stdout) (port-closed? stderr))]
            [deadline (throw 'timeout)]
            [else (lp #f)]))
         [#(stdout ,prev-os-pid ,x)
          (cond
           [(eof-object? x)
            (close-port stdin)]
           [else
            (display-string x stdin)
            (newline stdin)])
          (lp deadline)]
         [#(stderr ,@os-pid ,x)
          (unless (eof-object? x)
            (fprintf (console-error-port) "[~a] ~a\n" os-pid x))
          (lp deadline)]
         [#(process-terminated ,@os-pid ,exit-status ,signal)
          (when (and check-exit-code? (not (zero? exit-status)))
            (fprintf (console-error-port) "[~a] ~a failed with exit code ~s / signal ~s\n"
              os-pid cmd exit-status signal)
            ;; TODO can we scoop up stdout text and use that as our reason?
            (kill next-pid 'prior-command-failed))
          (lp #f)]
         [`(EXIT ,pid ,reason)
          (when (eq? reason 'prior-command-failed)
            (kill next-pid reason))
          (if (or (eq? pid self) (eq? pid next-pid))
              ;; TODO maybe we should try to close ports here
              (osi_kill* os-pid SIGTERM)
              (lp deadline))]))))

  (define (relay what dest-pid)
    (define (transmit line)
      (LOG 2 "send ~s ~s\n" dest-pid line)
      (send dest-pid `#(stdout relay ,line)))
    (cond
     [(list? what)
      (for-each transmit what)
      (transmit #!eof)]
     [(vector? what)
      (vector-for-each transmit what)
      (transmit #!eof)]
     [(string? what)
      (transmit what)
      (transmit #!eof)]
     [(procedure? what)
      (let ([deadline (+ (erlang:now) max-cmd-time)])
        (let lp ()
          (receive (until deadline (throw 'timeout))
            [#(stdout ,prev-os-pid ,x)
             (transmit (if (eof-object? x) x (what x)))
             (unless (eof-object? x) (lp))])))]
     [else (errorf #f "cannot relay ~s" what)]))

  (define (gather me)
    (process-trap-exit #t)
    (let lp ([lines '()])
      (receive
       [#(stdout ,prev-os-pid ,x)
        (if (eof-object? x)
            (send me `#(pipe-output ,(reverse lines)))
            (lp (cons x lines)))]
       [`(EXIT ,pid ,reason)
        (fprintf (console-error-port) "GATHER ~s EXIT ~s ~a\n" self pid reason)
        (send me `#(pipe-failed ,reason))])))

  (define-syntax ! (lambda (x) (syntax-error x "invalid context for")))

  (define-syntax wire
    (syntax-rules ($ !)
      [(_ gather) gather]
      [(_ ($ e0 e1 ...) next ...)
       (make-pipeline-command #t `e0 `(e1 ...)
         (spawn&link (lambda () (wire next ...))))]
      [(_ (! e0 e1 ...) next ...)
       ;; to ignore exit code in a case like grep
       (make-pipeline-command #f `e0 `(e1 ...)
         (spawn&link (lambda () (wire next ...))))]
      [(_ e0 next ...)
       (relay e0 (spawn&link (lambda () (wire next ...))))]))

  (define-syntax pipe
    (syntax-rules ()
      [(_ x ...)
       (let ([me self])
         (spawn&link
          (lambda ()
            (wire x ... (gather me))))
         (receive
          [#(pipe-output ,lines) lines]
          [#(pipe-failed ,reason) (raise reason)]))]))

  )

#!eof

> (pipe ($ ls) string-upcase ($ grep .ORG$) string-length (lambda (n) (make-string n #\x)))
("xxxxxxxxxx" "xxxxxxxxxxxxxxxxxxx" "xxxxxxxxx" "xxxxxxxxxxxxxx"
  "xxxxxxxxxxxx" "xxxxxxx" "xxxxxxxxxx"
  "xxxxxxxxxxxxxxxxxxxxx" "xxxxxxxxxxx" "xxxxxxxxxxxx"
  "xxxxxxxxxxxxxxxxxxxx" "xxxxxxxxxxx" "xxxxxxxxxxxxxxx"
  "xxxxxxxxxxxxxxxxxxx" "xxxxxxxxx" "xxxxxxxxxxx")
> (pipe (format "foo ~a" self) string-upcase)
("FOO #<PROCESS :6>")
> (pipe ($ ls) ($ grep "[.]ss$") string->list reverse list->string)
("ss.mb-gol-tla" "ss.tprecxe" "ss.bif" "ss.oof" "ss.huh" "ss.hcneb-oi"
  "ss.mb-gol" "ss.2pamp" "ss.emit-ehcac-tset" "ss.tset"
  "ss.rekcit")
