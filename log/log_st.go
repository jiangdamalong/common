package log

import (
	"fmt"
)

type Logger struct {
	Level     int
	StackPlus int
}

var LoggerIf = Logger{StackPlus: 0}

func (s *Logger) Panic(v ...interface{}) {
	if s.Level > 0 && s.Level > PanicLevel {
		StLogger.WriteLog(PanicLevel, s.StackPlus+1, v...)
	}
}

func (s *Logger) Panicf(sfmt string, v ...interface{}) {
	if s.Level > 0 && s.Level > PanicLevel {
		StLogger.WriteLogf(PanicLevel, s.StackPlus+1, sfmt, v...)
	}
}

func (s *Logger) Error(v ...interface{}) {
	if s.Level > 0 && s.Level > ErrorLevel {
		StLogger.WriteLog(ErrorLevel, s.StackPlus+1, v...)
	}
}

func (s *Logger) Errorf(sfmt string, v ...interface{}) {
	if s.Level > 0 && s.Level > ErrorLevel {
		StLogger.WriteLogf(ErrorLevel, s.StackPlus+1, sfmt, v...)
	}
}

func (s *Logger) Fatal(v ...interface{}) {
	if s.Level > 0 && s.Level > FatalLevel {
		StLogger.WriteLog(FatalLevel, s.StackPlus+1, v...)
	}
}

func (s *Logger) Fatalf(sfmt string, v ...interface{}) {
	if s.Level > 0 && s.Level > FatalLevel {
		StLogger.WriteLogf(FatalLevel, s.StackPlus+1, sfmt, v...)
	}
}

func (s *Logger) Info(v ...interface{}) {
	if s.Level > 0 && s.Level > InfoLevel {
		StLogger.WriteLog(InfoLevel, s.StackPlus+1, v...)
	}
}

func (s *Logger) Infof(sfmt string, v ...interface{}) {
	if s.Level > 0 && s.Level > InfoLevel {
		StLogger.WriteLogf(InfoLevel, s.StackPlus+1, sfmt, v...)
	}
}

func (s *Logger) Debug(v ...interface{}) {
	if s.Level > 0 && s.Level > DebugLevel {
		StLogger.WriteLog(DebugLevel, s.StackPlus+1, v...)
	}
}

func (s *Logger) Debugf(sfmt string, v ...interface{}) {
	if s.Level > 0 && s.Level > DebugLevel {
		StLogger.WriteLogf(DebugLevel, s.StackPlus+1, sfmt, v...)
	}
}

func (s *Logger) StartV(dir string, prefix string, climit int, chan_len int) {
	StLogger.Start(dir, prefix, climit, chan_len)
}

func (s *Logger) Start(dir string, prefix string) {
	StLogger.Start(dir, prefix, 100, 10000)
}

func (s *Logger) SetLevel(level int) {
	StLogger.SetLevel(level)
}

func (s *Logger) SetLevelString(level string) {
	StLogger.SetLevel(stringToLogLevel(level))
}

func (s *Logger) Print(v ...interface{}) {
	fmt.Println(v...)
	Debug(v...)
}

func (s *Logger) Println(v ...interface{}) {
	Print(v...)
}

func (s *Logger) Printf(sfmt string, v ...interface{}) {
	fmt.Printf(sfmt+"\r\n", v...)
}

func (s *Logger) OpenPrint() {
	StLogger.OpenPrint()
}

func (s *Logger) ClosePrint() {
	StLogger.ClosePrint()
}

func (s *Logger) Warn(v ...interface{}) {
	if s.Level > 0 && s.Level > WarnLevel {
		StLogger.WriteLog(WarnLevel, s.StackPlus+1, v...)
	}
}

func (s *Logger) Warnf(sfmt string, v ...interface{}) {
	if s.Level > 0 && s.Level > WarnLevel {
		StLogger.WriteLogf(WarnLevel, s.StackPlus+1, sfmt, v...)
	}
}

func (s *Logger) Flow(v ...interface{}) {
	if s.Level > 0 && s.Level > FlowLevel {
		StLogger.WriteLog(FlowLevel, s.StackPlus+1, v...)
	}
}

func (s *Logger) Flowf(sfmt string, v ...interface{}) {
	if s.Level > 0 && s.Level > FlowLevel {
		StLogger.WriteLogf(FlowLevel, s.StackPlus+1, sfmt, v...)
	}
}
