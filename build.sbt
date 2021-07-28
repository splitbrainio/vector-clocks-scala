import BuildKeys._
import Boilerplate._

import sbtcrossproject.CrossPlugin.autoImport.{crossProject, CrossType}
import sbtcrossproject.CrossProject

// ---------------------------------------------------------------------------
// Commands


/* We have no other way to target only JVM or JS projects in tests. */
lazy val aggregatorIDs = Seq("core")

addCommandAlias("ci-jvm",     ";" + aggregatorIDs.map(id => s"${id}JVM/clean ;${id}JVM/test:compile ;${id}JVM/test").mkString(";"))
addCommandAlias("ci-js",      ";" + aggregatorIDs.map(id => s"${id}JS/clean ;${id}JS/test:compile ;${id}JS/test").mkString(";"))
addCommandAlias("ci-package", ";scalafmtCheckAll ;package")
addCommandAlias("ci-doc",     ";unidoc ;site/makeMicrosite")
addCommandAlias("ci",         ";project root ;reload ;+scalafmtCheckAll ;+ci-jvm ;+ci-js ;+package ;ci-doc")
addCommandAlias("release",    ";+clean ;ci-release ;unidoc ;site/publishMicrosite")

// ---------------------------------------------------------------------------
// Dependencies

/** Standard FP library for Scala:
  * [[https://typelevel.org/cats/]]
  */
val CatsVersion = "2.3.0"

/** FP library for describing side-effects:
  * [[https://typelevel.org/cats-effect/]]
  */
val CatsEffectVersion = "2.3.0"

/** First-class support for type-classes:
  * [[https://github.com/typelevel/simulacrum]]
  */
val SimulacrumVersion = "1.0.0"

/** For macros that are supported on older Scala versions.
  * Not needed starting with Scala 2.13.
  */
val MacroParadiseVersion = "2.1.1"

/** Library for unit-testing:
  * [[https://github.com/monix/minitest/]]
  *  - [[https://github.com/scalatest/scalatest]]
  *  - [[https://github.com/scalatest/scalatestplus-scalacheck/]]
  */
val ScalaTestVersion = "3.2.3"
val ScalaTestPlusVersion = "3.2.3.0"

/** Library for property-based testing:
  * [[https://www.scalacheck.org/]]
  */
val ScalaCheckVersion = "1.15.1"

/** Library for doing property based checking of typeclass laws:
  * [[https://github.com/typelevel/discipline-scalatest]]
  */
val DisciplineScalaTestVersion = "2.1.0"

/** Compiler plugin for working with partially applied types:
  * [[https://github.com/typelevel/kind-projector]]
  */
val KindProjectorVersion = "0.11.0"

/** Compiler plugin for fixing "for comprehensions" to do desugaring w/o `withFilter`:
  * [[https://github.com/typelevel/kind-projector]]
  */
val BetterMonadicForVersion = "0.3.1"

/** Compiler plugin for silencing compiler warnings:
  * [[https://github.com/ghik/silencer]]
  */
val SilencerVersion = "1.7.1"

/** Used for publishing the microsite:
  * [[https://github.com/47degrees/github4s]]
  */
val GitHub4sVersion = "0.26.0"

/**
  * Provides Scala 2.13 collections API for 2.12 cross compilation
  */
val CollectionsCompatVersion = "2.3.2"

/**
  * Defines common plugins between all projects.
  */
def defaultPlugins: Project ⇒ Project = pr => {
  val withCoverage = sys.env.getOrElse("SBT_PROFILE", "") match {
    case "coverage" => pr
    case _ => pr.disablePlugins(scoverage.ScoverageSbtPlugin)
  }
  withCoverage
    .enablePlugins(AutomateHeaderPlugin)
    .enablePlugins(GitBranchPrompt)
}

lazy val sharedSettings = Seq(
  projectTitle := "Vector Clocks",
  projectWebsiteRootURL := "https://splitbrain.io/",
  projectWebsiteBasePath := "/vector-clocks/",
  githubOwnerID := "hugofirth",
  githubRelativeRepositoryID := "vector-clocks",

  organization := "io.splitbrain",
  scalaVersion := "2.13.3",
  crossScalaVersions := Seq("2.12.12", "2.13.3"),

  // More version specific compiler options
  scalacOptions ++= (CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, v)) if v >= 13 =>
      Seq(
        // Replaces macro-paradise in Scala 2.13
        // No
        //"-Ymacro-annotations",
      )
    case _ =>
      Seq.empty
  }),

  // Turning off fatal warnings for doc generation
  scalacOptions.in(Compile, doc) ~= filterConsoleScalacOptions,
  // Silence all warnings from src_managed files
  scalacOptions += "-P:silencer:pathFilters=.*[/]src_managed[/].*",

  addCompilerPlugin("org.typelevel" % "kind-projector" % KindProjectorVersion cross CrossVersion.full),
  addCompilerPlugin("com.olegpy" %% "better-monadic-for" % BetterMonadicForVersion),
  addCompilerPlugin("com.github.ghik" % "silencer-plugin" % SilencerVersion cross CrossVersion.full),

  // ScalaDoc settings
  autoAPIMappings := true,
  scalacOptions in ThisBuild ++= Seq(
    // Note, this is used by the doc-source-url feature to determine the
    // relative path of a given source file. If it's not a prefix of a the
    // absolute path of the source file, the absolute path of that file
    // will be put into the FILE_SOURCE variable, which is
    // definitely not what we want.
    "-sourcepath", file(".").getAbsolutePath.replaceAll("[.]$", "")
  ),

  // https://github.com/sbt/sbt/issues/2654
  incOptions := incOptions.value.withLogRecompileOnMacro(false),

  // ---------------------------------------------------------------------------
  // Options for testing

  logBuffered in Test := false,
  logBuffered in IntegrationTest := false,

  // ---------------------------------------------------------------------------
  // Options meant for publishing on Maven Central

  publishArtifact in Test := false,
  pomIncludeRepository := { _ => false }, // removes optional dependencies

  licenses := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
  homepage := Some(url(projectWebsiteFullURL.value)),
  headerLicense := Some(HeaderLicense.Custom(
    s"""|Copyright (c) 2021 the ${projectTitle.value} contributors.
        |See the project homepage at: ${projectWebsiteFullURL.value}
        |
        |Licensed under the Apache License, Version 2.0 (the "License");
        |you may not use this file except in compliance with the License.
        |You may obtain a copy of the License at
        |
        |    http://www.apache.org/licenses/LICENSE-2.0
        |
        |Unless required by applicable law or agreed to in writing, software
        |distributed under the License is distributed on an "AS IS" BASIS,
        |WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
        |See the License for the specific language governing permissions and
        |limitations under the License."""
      .stripMargin)),

  scmInfo := Some(
    ScmInfo(
      url(s"https://github.com/${githubFullRepositoryID.value}"),
      s"scm:git@github.com:${githubFullRepositoryID.value}.git"
    )),

  developers := List(
    Developer(
      id="hugofirth",
      name="Hugo Firth",
      email="hugo@splitbrain.io",
      url=url("https://splitbrain.io")
    )),

  // -- Settings meant for deployment on oss.sonatype.org
  sonatypeProfileName := organization.value,
)

/**
  * Shared configuration across all sub-projects with actual code to be published.
  */
def defaultCrossProjectConfiguration(pr: CrossProject) = {
  val sharedJavascriptSettings = Seq(
    coverageExcludedFiles := ".*",
    // Use globally accessible (rather than local) source paths in JS source maps
    scalacOptions += {
      val tagOrHash = {
        val ver = s"v${version.value}"
        if (isSnapshot.value)
          git.gitHeadCommit.value.getOrElse(ver)
        else
          ver
      }
      val l = (baseDirectory in LocalRootProject).value.toURI.toString
      val g = s"https://raw.githubusercontent.com/${githubFullRepositoryID.value}/$tagOrHash/"
      s"-P:scalajs:mapSourceURI:$l->$g"
    },
    // Needed in order to publish for multiple Scala.js versions:
    // https://github.com/olafurpg/sbt-ci-release#how-do-i-publish-cross-built-scalajs-projects
    skip.in(publish) := customScalaJSVersion.isEmpty,
  )

  val sharedJVMSettings = Seq(
    // Needed in order to publish for multiple Scala.js versions:
    // https://github.com/olafurpg/sbt-ci-release#how-do-i-publish-cross-built-scalajs-projects
    skip.in(publish) := customScalaJSVersion.isDefined,
  )

  pr.configure(defaultPlugins)
    .settings(sharedSettings)
    .jsSettings(sharedJavascriptSettings)
    .jvmSettings(doctestTestSettings(DoctestTestFramework.ScalaTest))
    .jvmSettings(sharedJVMSettings)
    .settings(crossVersionSharedSources)
    .settings(requiredMacroCompatDeps(MacroParadiseVersion))
    .settings(filterOutMultipleDependenciesFromGeneratedPomXml(
      "groupId" -> "org.scoverage".r :: Nil,
      "groupId" -> "org.typelevel".r :: "artifactId" -> "simulacrum".r :: Nil,
    ))
}

lazy val root = project.in(file("."))
  .enablePlugins(ScalaUnidocPlugin)
  .aggregate(coreJVM, coreJS)
  .configure(defaultPlugins)
  .settings(sharedSettings)
  .settings(doNotPublishArtifact)
  .settings(unidocSettings(coreJVM))
  .settings(
    // Try really hard to not execute tasks in parallel ffs
    Global / concurrentRestrictions := Tags.limitAll(1) :: Nil,
  )

lazy val site = project.in(file("site"))
  .disablePlugins(MimaPlugin)
  .enablePlugins(MicrositesPlugin)
  .enablePlugins(MdocPlugin)
  .settings(sharedSettings)
  .settings(doNotPublishArtifact)
  .dependsOn(coreJVM)
  .settings {
    import microsites._
    Seq(
      micrositeName := projectTitle.value,
      micrositeDescription := "Vector clocks for your distributed systems",
      micrositeAuthor := "Hugo Firth",
      micrositeTwitterCreator := "@hugofirth",
      micrositeGithubOwner := githubOwnerID.value,
      micrositeGithubRepo := githubRelativeRepositoryID.value,
      micrositeUrl := projectWebsiteRootURL.value.replaceAll("[/]+$", ""),
      micrositeBaseUrl := projectWebsiteBasePath.value.replaceAll("[/]+$", ""),
      micrositeDocumentationUrl := s"${projectWebsiteFullURL.value.replaceAll("[/]+$", "")}/${docsMappingsAPIDir.value}/",
      micrositeGitterChannelUrl := githubFullRepositoryID.value,
      micrositeFooterText := None,
      micrositeHighlightTheme := "atom-one-light",
      micrositePalette := Map(
        "brand-primary" -> "#3e5b95",
        "brand-secondary" -> "#294066",
        "brand-tertiary" -> "#2d5799",
        "gray-dark" -> "#49494B",
        "gray" -> "#7B7B7E",
        "gray-light" -> "#E5E5E6",
        "gray-lighter" -> "#F4F3F4",
        "white-color" -> "#FFFFFF"
      ),
      micrositeCompilingDocsTool := WithMdoc,
      fork in mdoc := true,
      scalacOptions.in(Tut) ~= filterConsoleScalacOptions,
      libraryDependencies += "com.47deg" %% "github4s" % GitHub4sVersion,
      micrositePushSiteWith := GitHub4s,
      micrositeGithubToken := sys.env.get("GITHUB_TOKEN"),
      micrositeExtraMdFilesOutput := (resourceManaged in Compile).value / "jekyll",
      micrositeConfigYaml := ConfigYml(
        yamlPath = Some((resourceDirectory in Compile).value / "microsite" / "_config.yml")
      ),
      micrositeExtraMdFiles := Map(
        file("README.md") -> ExtraMdFileConfig("index.md", "page", Map("title" -> "Home", "section" -> "home", "position" -> "100")),
        file("CHANGELOG.md") -> ExtraMdFileConfig("CHANGELOG.md", "page", Map("title" -> "Change Log", "section" -> "changelog", "position" -> "101")),
        file("CONTRIBUTING.md") -> ExtraMdFileConfig("CONTRIBUTING.md", "page", Map("title" -> "Contributing", "section" -> "contributing", "position" -> "102")),
        file("CODE_OF_CONDUCT.md") -> ExtraMdFileConfig("CODE_OF_CONDUCT.md", "page", Map("title" -> "Code of Conduct", "section" -> "code of conduct", "position" -> "103")),
        file("LICENSE.md") -> ExtraMdFileConfig("LICENSE.md", "page", Map("title" -> "License", "section" -> "license", "position" -> "104")),
      ),
      docsMappingsAPIDir := s"api",
      addMappingsToSiteDir(mappings in (ScalaUnidoc, packageDoc) in root, docsMappingsAPIDir),
      sourceDirectory in Compile := baseDirectory.value / "src",
      sourceDirectory in Test := baseDirectory.value / "test",
      mdocIn := (sourceDirectory in Compile).value / "mdoc",

      run in Compile := {
        import scala.sys.process._

        val s: TaskStreams = streams.value
        val shell: Seq[String] = if (sys.props("os.name").contains("Windows")) Seq("cmd", "/c") else Seq("bash", "-c")

        val jekyllServe: String = s"jekyll serve --open-url --baseurl ${(micrositeBaseUrl in Compile).value}"

        s.log.info("Running Jekyll...")
        Process(shell :+ jekyllServe, (micrositeExtraMdFilesOutput in Compile).value) !
      },
    )
  }

lazy val core = crossProject(JSPlatform, JVMPlatform)
  .crossType(CrossType.Full)
  .in(file("core"))
  .configureCross(defaultCrossProjectConfiguration)
  .settings(
    name := "vector-clocks-core",
    libraryDependencies ++= Seq(
      "org.typelevel"  %%% "simulacrum"       % SimulacrumVersion % Provided,
      "org.typelevel"  %%% "cats-core"        % CatsVersion,
      "org.typelevel"  %%% "cats-effect"      % CatsEffectVersion,
      // For testing
      "org.scala-lang.modules" %%% "scala-collection-compat" % CollectionsCompatVersion % Test,
      "org.scalatest"          %%% "scalatest"               % ScalaTestVersion % Test,
      "org.scalatestplus"      %%% "scalacheck-1-15"         % ScalaTestPlusVersion % Test,
      "org.scalacheck"         %%% "scalacheck"              % ScalaCheckVersion % Test,
      "org.typelevel"          %%% "cats-laws"               % CatsVersion % Test,
      "org.typelevel"          %%% "cats-effect-laws"        % CatsEffectVersion % Test,
      "org.typelevel"          %%% "discipline-scalatest"    % DisciplineScalaTestVersion % Test,
    ),
  )

lazy val coreJVM = core.jvm
lazy val coreJS  = core.js

// Reloads build.sbt changes whenever detected
Global / onChangedBuildSource := ReloadOnSourceChanges
