package net.corda.sandbox.analysis

import com.sun.org.apache.xml.internal.serializer.utils.Utils.messages
import net.corda.sandbox.code.EmitterModule
import net.corda.sandbox.code.Instruction
import net.corda.sandbox.code.instructions.*
import net.corda.sandbox.messages.Message
import net.corda.sandbox.references.Class
import net.corda.sandbox.references.ClassReference
import net.corda.sandbox.references.Member
import net.corda.sandbox.references.MemberReference
import net.corda.sandbox.source.SourceClassLoader
import org.objectweb.asm.*
import java.io.InputStream

/**
 * Functionality for traversing a class and its members.
 *
 * @property classVisitor Class visitor to use when traversing the structure of classes.
 * @property configuration The configuration to use for the analysis
 */
open class ClassAndMemberVisitor(
        private val classVisitor: ClassVisitor? = null,
        private val configuration: AnalysisConfiguration = AnalysisConfiguration()
) {

    /**
     * Holds a reference to the currently used analysis context.
     */
    protected var analysisContext: AnalysisContext =
            AnalysisContext.fromConfiguration(configuration, emptyList())

    /**
     * Holds a link to the class currently being traversed.
     */
    private var currentClass: Class? = null

    /**
     * Holds a link to the member currently being traversed.
     */
    private var currentMember: Member? = null

    /**
     * The current source location.
     */
    private var sourceLocation = SourceLocation()

    /**
     * The class loader used to find classes on the extended class path.
     */
    private val supportingClassLoader =
            SourceClassLoader(configuration.classPath, configuration.classResolver)

    /**
     * Analyze class by using the provided qualified name of the class.
     */
    inline fun <reified T> analyze(context: AnalysisContext) = analyze(T::class.java.name, context)

    /**
     * Analyze class by using the provided qualified name of the class.
     *
     * @param className The full, qualified name of the class.
     * @param context The context in which the analysis is conducted.
     * @param origin The originating class for the analysis.
     */
    fun analyze(className: String, context: AnalysisContext, origin: String? = null) {
        supportingClassLoader.classReader(className, context, origin).apply {
            analyze(this, context)
        }
    }

    /**
     * Analyze class by using the provided stream of its byte code.
     *
     * @param classStream A stream of the class' byte code.
     * @param context The context in which the analysis is conducted.
     */
    fun analyze(classStream: InputStream, context: AnalysisContext) =
            analyze(ClassReader(classStream), context)

    /**
     * Analyze class by using the provided class reader.
     *
     * @param classReader An instance of the class reader to use to access the byte code.
     * @param context The context in which to analyse the provided class.
     * @param options Options for how to parse and process the class.
     */
    fun analyze(classReader: ClassReader, context: AnalysisContext, options: Int = 0) {
        analysisContext = context
        classReader.accept(ClassVisitorImpl(classVisitor), options)
    }

    /**
     * Extract information about the traversed class.
     */
    open fun visitClass(clazz: Class) = clazz

    /**
     * Process class after it has been fully traversed and analyzed.
     */
    open fun visitClassEnd(clazz: Class) {}

    /**
     * Extract the meta-data indicating the source file of the traversed class (i.e., where it is compiled from).
     */
    open fun visitSource(clazz: Class, source: String) {}

    /**
     * Extract information about the traversed class annotation.
     */
    open fun visitClassAnnotation(clazz: Class, descriptor: String) {}

    /**
     * Extract information about the traversed member annotation.
     */
    open fun visitMemberAnnotation(clazz: Class, member: Member, descriptor: String) {}

    /**
     * Extract information about the traversed method.
     */
    open fun visitMethod(clazz: Class, method: Member) = method

    /**
     * Extract information about the traversed field.
     */
    open fun visitField(clazz: Class, field: Member) = field

    /**
     * Extract information about the traversed instruction.
     */
    open fun visitInstruction(method: Member, emitter: EmitterModule, instruction: Instruction) {}

    /**
     * Get the analysis context to pass on to method and field visitors.
     */
    protected fun currentAnalysisContext() = AnalysisRuntimeContext(
            currentClass!!,
            currentMember,
            sourceLocation,
            analysisContext.messages,
            configuration
    )

    /**
     * Check if a class should be processed or not.
     */
    protected fun shouldBeProcessed(className: String) =
            !configuration.whitelist.inNamespace(className) && !configuration.fixedClasses.matches(className)

    /**
     * Extract information about the traversed member annotation.
     */
    private fun visitMemberAnnotation(
            descriptor: String, referencedClass: Class? = null, referencedMember: Member? = null
    ) {
        val clazz = (referencedClass ?: currentClass) ?: return
        val member = (referencedMember ?: currentMember) ?: return
        member.annotations.add(descriptor)
        withGuard {
            visitMemberAnnotation(clazz, member, descriptor)
        }
    }

    /**
     * Run action with a guard that populates [messages] based on the output.
     */
    private inline fun withGuard(action: () -> Unit) = try {
        action()
        true
    } catch (exception: Throwable) {
        recordMessage(exception, currentAnalysisContext())
        false
    }

    /**
     * Record a message derived from a [Throwable].
     */
    private fun recordMessage(exception: Throwable, context: AnalysisRuntimeContext) {
        context.messages.add(Message.fromThrowable(exception, context.location))
    }

    /**
     * Record a reference to a class.
     */
    private fun addReferenceToType(type: String) {
        val typeName = configuration.classModule
                .normalizeClassName(type)
                .replace("[]", "")
        if (shouldBeProcessed(currentClass!!.name)) {
            val classReference = ClassReference(typeName)
            analysisContext.references.add(classReference, sourceLocation)
        }
    }

    /**
     * Record a reference to a class member.
     */
    private fun addReferenceToMember(owner: String, name: String, desc: String) {
        if (shouldBeProcessed(currentClass!!.name)) {
            addReferenceToType(owner)
            val memberReference = MemberReference(owner, name, desc)
            analysisContext.references.add(memberReference, sourceLocation)
        }
    }

    /**
     * Visitor used to traverse and analyze a class.
     */
    private inner class ClassVisitorImpl(
            targetVisitor: ClassVisitor?
    ) : ClassVisitor(API_VERSION, targetVisitor) {

        /**
         * Extract information about the traversed class.
         */
        override fun visit(
                version: Int, access: Int, name: String, signature: String?, superName: String?,
                interfaces: Array<out String>?
        ) {
            val superClassName = superName ?: ""
            val interfaceNames = interfaces?.toMutableList() ?: mutableListOf()
            Class(version, access, name, superClassName, interfaceNames, genericsDetails = signature ?: "").apply {
                currentClass = this
                currentMember = null
                sourceLocation = SourceLocation(
                        className = name
                )
            }
            withGuard {
                currentClass = visitClass(currentClass!!)
            }
            val visitedClass = currentClass!!
            analysisContext.classes.add(visitedClass)
            super.visit(
                    version, access, visitedClass.name, signature,
                    visitedClass.superClass.let { return@let if (it.isEmpty()) null else it },
                    visitedClass.interfaces.toTypedArray()
            )
        }

        /**
         * Post-processing of the traversed class.
         */
        override fun visitEnd() {
            configuration.classModule
                    .getClassReferencesFromClass(currentClass!!, configuration.analyzeAnnotations)
                    .forEach { addReferenceToType(it) }
            withGuard {
                visitClassEnd(currentClass!!)
            }
            super.visitEnd()
        }

        /**
         * Extract the meta-data indicating the source file of the traversed class (i.e., where it is compiled from).
         */
        override fun visitSource(source: String?, debug: String?) {
            currentClass?.apply {
                sourceFile = configuration.classModule.getFullSourceLocation(this, source)
                sourceLocation = sourceLocation.copy(sourceFile = sourceFile)
                withGuard {
                    visitSource(this, sourceFile)
                }
            }
            super.visitSource(source, debug)
        }

        /**
         * Extract information about provided annotations.
         */
        override fun visitAnnotation(desc: String, visible: Boolean): AnnotationVisitor? {
            currentClass?.apply {
                annotations.add(desc)
                withGuard {
                    visitClassAnnotation(this, desc)
                }
            }
            return super.visitAnnotation(desc, visible)
        }

        /**
         * Extract information about the traversed method.
         */
        override fun visitMethod(
                access: Int, name: String, desc: String, signature: String?, exceptions: Array<out String>?
        ): MethodVisitor? {
            var visitedMember: Member? = null
            var processMember = true
            currentClass?.apply {
                val member = Member(access, this.name, name, desc, signature ?: "")
                currentMember = member
                sourceLocation = sourceLocation.copy(
                        memberName = name,
                        signature = desc,
                        lineNumber = 0
                )
                processMember = withGuard {
                    visitedMember = visitMethod(this, member)
                }
                configuration.memberModule.addToClass(this, visitedMember ?: member)
            }
            return if (processMember) {
                val member = visitedMember ?: currentMember!!
                val targetVisitor = super.visitMethod(
                        member.access,
                        member.memberName,
                        member.signature,
                        signature,
                        member.exceptions.toTypedArray()
                )
                MethodVisitorImpl(targetVisitor)
            } else {
                null
            }
        }

        /**
         * Extract information about the traversed field.
         */
        override fun visitField(
                access: Int, name: String, desc: String, signature: String?, value: Any?
        ): FieldVisitor? {
            var visitedMember: Member? = null
            var processMember = true
            currentClass?.apply {
                val member = Member(access, this.name, name, desc, "", value = value)
                currentMember = member
                sourceLocation = sourceLocation.copy(
                        memberName = name,
                        signature = desc,
                        lineNumber = 0
                )
                processMember = withGuard {
                    visitedMember = visitField(this, member)
                }
                configuration.memberModule.addToClass(this, visitedMember ?: member)
            }
            return if (processMember) {
                val member = visitedMember ?: currentMember!!
                val targetVisitor = super.visitField(
                        member.access,
                        member.memberName,
                        member.signature,
                        signature,
                        member.value
                )
                FieldVisitorImpl(targetVisitor)
            } else {
                null
            }
        }

    }

    /**
     * Visitor used to traverse and analyze a method.
     */
    private inner class MethodVisitorImpl(
            targetVisitor: MethodVisitor?
    ) : MethodVisitor(API_VERSION, targetVisitor) {

        /**
         * Record line number of current instruction.
         */
        override fun visitLineNumber(line: Int, start: Label?) {
            sourceLocation = sourceLocation.copy(lineNumber = line)
            super.visitLineNumber(line, start)
        }

        /**
         * Extract information about provided label.
         */
        override fun visitLabel(label: Label) {
            visit(CodeLabel(label), defaultFirst = true) {
                super.visitLabel(label)
            }
        }

        /**
         * Extract information about provided annotations.
         */
        override fun visitAnnotation(desc: String, visible: Boolean): AnnotationVisitor? {
            visitMemberAnnotation(desc)
            return super.visitAnnotation(desc, visible)
        }

        /**
         * Extract information about provided field access instruction.
         */
        override fun visitFieldInsn(opcode: Int, owner: String, name: String, desc: String) {
            addReferenceToMember(owner, name, desc)
            visit(MemberAccessInstruction(opcode, owner, name, desc, isMethod = false)) {
                super.visitFieldInsn(opcode, owner, name, desc)
            }
        }

        /**
         * Extract information about provided method invocation instruction.
         */
        override fun visitMethodInsn(opcode: Int, owner: String, name: String, desc: String, itf: Boolean) {
            addReferenceToMember(owner, name, desc)
            visit(MemberAccessInstruction(opcode, owner, name, desc, itf, isMethod = true)) {
                super.visitMethodInsn(opcode, owner, name, desc, itf)
            }
        }

        /**
         * Extract information about provided dynamic invocation instruction.
         */
        override fun visitInvokeDynamicInsn(name: String, desc: String, bsm: Handle?, vararg bsmArgs: Any?) {
            val module = configuration.memberModule
            visit(DynamicInvocationInstruction(
                    name, desc, module.numberOfArguments(desc), module.returnsValueOrReference(desc)
            )) {
                super.visitInvokeDynamicInsn(name, desc, bsm, *bsmArgs)
            }
        }

        /**
         * Extract information about provided jump instruction.
         */
        override fun visitJumpInsn(opcode: Int, label: Label) {
            visit(BranchInstruction(opcode, label)) {
                super.visitJumpInsn(opcode, label)
            }
        }

        /**
         * Extract information about provided instruction (general instruction with no operands).
         */
        override fun visitInsn(opcode: Int) {
            visit(Instruction(opcode)) {
                super.visitInsn(opcode)
            }
        }

        /**
         * Extract information about provided instruction (general instruction with one operand).
         */
        override fun visitIntInsn(opcode: Int, operand: Int) {
            visit(IntegerInstruction(opcode, operand)) {
                super.visitIntInsn(opcode, operand)
            }
        }

        /**
         * Extract information about provided type instruction (e.g., [Opcodes.NEW], [Opcodes.ANEWARRAY],
         * [Opcodes.INSTANCEOF] and [Opcodes.CHECKCAST]).
         */
        override fun visitTypeInsn(opcode: Int, type: String) {
            addReferenceToType(type)
            visit(TypeInstruction(opcode, type)) {
                try {
                    super.visitTypeInsn(opcode, type)
                } catch (exception: IllegalArgumentException) {
                    throw IllegalArgumentException("Invalid name used in type instruction; $type", exception)
                }
            }
        }

        /**
         * Extract information about provided try-catch/finally block.
         */
        override fun visitTryCatchBlock(start: Label, end: Label, handler: Label, type: String?) {
            visit(if (type != null) {
                TryCatchBlock(type, handler)
            } else {
                TryFinallyBlock(handler)
            }) {
                super.visitTryCatchBlock(start, end, handler, type)
            }
        }

        /**
         * Extract information about provided table switch instruction.
         */
        override fun visitTableSwitchInsn(min: Int, max: Int, dflt: Label, vararg labels: Label) {
            visit(TableSwitchInstruction(min, max, dflt, labels.toList())) {
                super.visitTableSwitchInsn(min, max, dflt, *labels)
            }
        }

        /**
         * Extract information about provided increment instruction.
         */
        override fun visitIincInsn(`var`: Int, increment: Int) {
            visit(IntegerInstruction(Opcodes.IINC, increment)) {
                super.visitIincInsn(`var`, increment)
            }
        }

        /**
         * Helper function used to streamline the access to an instruction and to catch any related processing errors.
         */
        private inline fun visit(instruction: Instruction, defaultFirst: Boolean = false, defaultAction: () -> Unit) {
            val emitterModule = EmitterModule(mv ?: StubMethodVisitor())
            if (defaultFirst) {
                defaultAction()
            }
            val success = withGuard {
                visitInstruction(currentMember!!, emitterModule, instruction)
            }
            if (!defaultFirst) {
                if (success && emitterModule.emitDefaultInstruction) {
                    defaultAction()
                }
            }
        }

    }

    /**
     * Visitor used to traverse and analyze a field.
     */
    private inner class FieldVisitorImpl(
            targetVisitor: FieldVisitor?
    ) : FieldVisitor(API_VERSION, targetVisitor) {

        /**
         * Extract information about provided annotations.
         */
        override fun visitAnnotation(desc: String, visible: Boolean): AnnotationVisitor? {
            visitMemberAnnotation(desc)
            return super.visitAnnotation(desc, visible)
        }

    }

    private inner class StubMethodVisitor : MethodVisitor(API_VERSION, null)

    companion object {

        /**
         * The API version of ASM.
         */
        const val API_VERSION = Opcodes.ASM6

    }

}
